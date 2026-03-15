from __future__ import annotations

import psutil
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any
from typing import Literal

import polars as pl
from pydantic import BaseModel, Field
from pydantic import field_validator
from pydantic import ValidationInfo
from vertex_forager.core.types import JSONValue
from vertex_forager.exceptions import VertexForagerError
from vertex_forager.constants import (
    FLUSH_THRESHOLD_ROWS,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_RETRY_BASE_BACKOFF_S,
    DEFAULT_RETRY_MAX_BACKOFF_S,
    HTTP_TIMEOUT_S,
    QUEUE_TARGET_RAM_RATIO,
    PACKET_SIZE_EST_BYTES,
    QUEUE_MIN,
    QUEUE_MAX,
    QUEUE_DEFAULT,
)
from collections.abc import Mapping


class RetryConfig(BaseModel):
    """Retry configuration for HTTP requests.

    Args:
        max_attempts: Maximum number of retry attempts (default: 3).
        base_backoff_s: Initial backoff duration in seconds (default: 1.0).
        max_backoff_s: Maximum backoff duration in seconds (default: 30.0).
        enable_http_status_retry: Toggle retry-on-HTTP-status behavior (default: True).
        retry_status_codes: Tuple of HTTP status codes to trigger retries (default: (429, 503)).

    Notes:
        - Backoff uses Full Jitter: sleep is drawn uniformly from [0, min(max_backoff_s, base_backoff_s * 2^(attempt-1))].
        - Defaults are conservative: retries on 429 (Too Many Requests) and 503 (Service Unavailable).
        - Opt-in to broader server errors (e.g., 500, 502, 504) ONLY when requests are idempotent.
          Non-idempotent operations (e.g., POST/PUT without idempotency keys) may cause duplicate side effects.
          Use idempotency keys or upstream idempotent semantics before enabling broader codes.
    """

    max_attempts: int = Field(default=DEFAULT_RETRY_MAX_ATTEMPTS, ge=1)
    base_backoff_s: float = Field(default=DEFAULT_RETRY_BASE_BACKOFF_S, ge=0.0)
    max_backoff_s: float = Field(default=DEFAULT_RETRY_MAX_BACKOFF_S, ge=0.0)
    enable_http_status_retry: bool = True
    retry_status_codes: tuple[int, ...] = (429, 503)

    @field_validator("max_backoff_s")
    @classmethod
    def _validate_backoff_window(cls, v: float, info: ValidationInfo) -> float:
        base = info.data.get("base_backoff_s", DEFAULT_RETRY_BASE_BACKOFF_S)
        if v < base:
            raise ValueError("max_backoff_s must be >= base_backoff_s")
        return v

    @field_validator("retry_status_codes")
    @classmethod
    def _validate_retry_codes(cls, v: tuple[int, ...]) -> tuple[int, ...]:
        if not v:
            return v
        for code in v:
            if code < 100 or code > 599:
                raise ValueError("retry_status_codes must be valid HTTP status codes (100-599)")
        return v


class HttpMethod(str, Enum):
    """HTTP method for request execution.

    Values:
        GET: HTTP GET method.
        POST: HTTP POST method.
    """

    GET = "GET"
    POST = "POST"


class RequestAuth(BaseModel):
    """Authentication strategy attached to a request spec.

    Args:
        kind: Authentication type (e.g., 'none', 'bearer', 'param') (default: 'none').
        token: Authentication token string if applicable (default: None).
        header_name: Name of the header to inject the token into (default: None).
        query_param: Name of the query parameter to inject the token into (default: None).
    """

    kind: Literal["none", "bearer", "header", "query"] = "none"
    token: str | None = None
    header_name: str | None = None
    query_param: str | None = None


class RequestSpec(BaseModel):
    """HTTP request specification for a fetch job.

    Args:
        method: HTTP method to use (default: HttpMethod.GET).
        url: Target URL for the request.
        params: Query parameters as key-value pairs (default: empty dict).
        headers: HTTP headers as key-value pairs (default: empty dict).
        json_body: JSON payload for POST/PUT requests (default: None).
        data: Raw bytes payload for requests (default: None).
        timeout_s: Request timeout in seconds (default: 30.0).
        auth: Authentication strategy to apply (default: RequestAuth()).
    """

    method: HttpMethod = HttpMethod.GET
    url: str
    params: dict[str, JSONValue] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: dict[str, JSONValue] | None = None
    data: bytes | None = None
    timeout_s: float = HTTP_TIMEOUT_S
    auth: RequestAuth = Field(default_factory=RequestAuth)

    @field_validator("params", mode="before")
    @classmethod
    def _validate_params(cls, v: Any) -> dict[str, JSONValue]:
        def _is_json_value(val: Any) -> bool:
            if isinstance(val, (str, int, float, bool)) or val is None:
                return True
            if isinstance(val, list):
                return all(_is_json_value(x) for x in val)
            if isinstance(val, dict):
                return all(isinstance(k, str) and _is_json_value(val[k]) for k in val)
            return False
        if not isinstance(v, dict):
            raise TypeError("params must be a dict[str, JSONValue]")
        for key, val in v.items():
            if not isinstance(key, str) or not _is_json_value(val):
                raise TypeError("params values must be JSON-serializable primitives/lists/dicts")
        return v


class FetchJob(BaseModel):
    """Unit of work for the fetch pipeline.

    Args:
        provider: Data provider name (e.g., 'sharadar').
        dataset: Dataset identifier (e.g., 'SEP', 'SF1').
        symbol: Target symbol or ticker if applicable (default: None).
        spec: HTTP request specification details.
        context: Additional context for job execution and tracing (default: empty dict).
    """

    provider: str
    dataset: str
    symbol: str | None = None
    spec: RequestSpec
    context: Mapping[str, JSONValue] = Field(default_factory=dict)


class FramePacket(BaseModel):
    """Polars frame packet passed from provider to sink.

    Args:
        provider: Data provider name.
        table: Target table name for storage.
        frame: Polars DataFrame containing the data.
        observed_at: Timestamp when the data was observed/fetched.
        partition_date: Optional date for partitioning logic (default: None).
        context: Metadata context passed along with the data (default: empty dict).
    """

    provider: str
    table: str
    frame: pl.DataFrame
    observed_at: datetime
    partition_date: date | None = None
    context: Mapping[str, JSONValue] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}


class EngineConfig(BaseModel):
    """Unified pipeline execution configuration (Simple & Flat).

    Consolidates all tuning parameters into a single configuration object.
    Automatically calculates optimal concurrency based on RPM if not provided.

    Attribute Groups:
        Core: requests_per_minute, concurrency
        Retry: retry
        Storage & Flush: flush_threshold_rows, writer_chunk_rows
        Observability: metrics_enabled, structured_logs, log_verbose
        DLQ: dlq_enabled, dlq_tmp_cleanup_on_error, dlq_tmp_periodic_cleanup, dlq_tmp_retention_s
        Adaptive Downshift: downshift_enabled, downshift_window_s, error_rate_threshold, rpm_floor, recovery_step, healthy_window_s

    Attributes:
        requests_per_minute (int): Maximum allowed requests per minute; must be > 0.
        concurrency (int | None): Explicit concurrency limit; if None, executor derives a safe value.
        retry (RetryConfig): Retry/backoff policy (attempts, backoff window, status codes).
        flush_threshold_rows (int): Rows buffered per table before flush; higher = fewer large flushes.
        writer_chunk_rows (int | None): Target per‑chunk rows during flush; when set must be >= 10_000.
        metrics_enabled (bool): Emit counters/histograms when True.
        structured_logs (bool): Emit structured stage logs when True.
        log_verbose (bool): Increase logging verbosity when True.
        dlq_enabled (bool): Enable on‑disk DLQ spooling; when False, files are not written and summaries/counts still populate.
        dlq_tmp_cleanup_on_error (bool): Attempt cleanup of temporary DLQ files on writer errors.
        dlq_tmp_periodic_cleanup (bool): Periodically clean temporary DLQ spool artifacts.
        dlq_tmp_retention_s (int): Retention window (seconds) for temporary DLQ artifacts (default 86_400 = 1 day).
        downshift_enabled (bool): Enable adaptive RPM downshift based on error rates.
        downshift_window_s (int): Sliding window (seconds) to evaluate error rate for downshift.
        error_rate_threshold (float): Error rate threshold (0..1) to trigger downshift.
        rpm_floor (int): Minimum RPM when downshift is active; must be <= requests_per_minute.
        recovery_step (int): RPM increment when recovering from downshift.
        healthy_window_s (int): Window (seconds) of healthy operation before stepping up RPM.

    Raises:
        ValueError: If requests_per_minute is not positive.
    """

    # 1. Core Parameters
    requests_per_minute: int = Field(..., gt=0)
    concurrency: int | None = Field(default=None, gt=0)

    # 2. Retry Configuration
    retry: RetryConfig = Field(default_factory=RetryConfig)

    # 3. Advanced Tuning (Internal Defaults)
    flush_threshold_rows: int = FLUSH_THRESHOLD_ROWS
    writer_chunk_rows: int | None = None
    metrics_enabled: bool = False
    structured_logs: bool = False
    log_verbose: bool = False
    dlq_enabled: bool = True
    dlq_tmp_cleanup_on_error: bool = True
    dlq_tmp_periodic_cleanup: bool = True
    dlq_tmp_retention_s: int = Field(default=86400, ge=0)

    # 4. Adaptive RPM Downshift
    downshift_enabled: bool = False
    downshift_window_s: int = Field(default=60, ge=1)
    error_rate_threshold: float = Field(default=0.2, ge=0.0, le=1.0)
    rpm_floor: int = Field(default=1, ge=1)
    recovery_step: int = Field(default=5, ge=1)
    healthy_window_s: int = Field(default=60, ge=1)

    @property
    def fetch_concurrency(self) -> int | None:
        """Alias for concurrency to maintain semantic clarity.

        Returns:
            int | None: The configured concurrency limit.
        """
        return self.concurrency

    @property
    def queue_max(self) -> int:
        """Calculate max queue size based on available system memory.
        Target: 5% of Total System RAM.

        Returns:
            int: Calculated maximum queue size (clamped between 100 and 2000).
        """
        try:
            total_ram = psutil.virtual_memory().total
            target_buffer_bytes = total_ram * QUEUE_TARGET_RAM_RATIO
            if PACKET_SIZE_EST_BYTES <= 0:
                raise ValueError("PACKET_SIZE_EST_BYTES must be > 0")
            calc_size = int(target_buffer_bytes / PACKET_SIZE_EST_BYTES)
            return max(QUEUE_MIN, min(QUEUE_MAX, calc_size))
        except (ValueError, AttributeError):
            return QUEUE_DEFAULT

    def assert_valid(self) -> None:
        """Validate configuration values.

        Raises:
            ValueError: If requests_per_minute is less than or equal to 0.
        """
        if self.requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        if self.concurrency is not None and self.concurrency <= 0:
            raise ValueError("concurrency must be positive if specified")
        if self.writer_chunk_rows is not None:
            try:
                v = int(self.writer_chunk_rows)
            except (TypeError, ValueError) as e:
                raise VertexForagerError(f"writer_chunk_rows must be an integer or None: {e}") from e
            if v < 10_000:
                raise ValueError("writer_chunk_rows must be >= 10_000 when specified")
            # Coerce to int for downstream isinstance checks and consistent typing
            self.writer_chunk_rows = v
        if self.rpm_floor > self.requests_per_minute:
            raise ValueError("rpm_floor must be <= requests_per_minute")


class RunResult(BaseModel):
    """Result summary for a pipeline run.

    Args:
        provider: Data provider name.
        tables: Dictionary mapping table names to row counts (default: empty dict).
        errors: List of error messages encountered (default: empty list).
        dlq_pending: Packets preserved for post-mortem/dead-letter processing when DLQ spool/dispatch fails.
            Keys are table names and values are lists of FramePacket instances. Items are appended by
            writer/rescue logic upon spool errors and can be consumed by operator recovery flows.
        dlq_counts: Per-table counts for rescued and remaining packets when DLQ is disabled or spooling occurs.
            Always populated by the pipeline regardless of metrics settings to provide a minimal summary.
    """

    provider: str
    tables: dict[str, int] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    metrics_counters: dict[str, int] = Field(default_factory=dict)
    metrics_histograms: dict[str, list[float]] = Field(default_factory=dict)
    metrics_summary: dict[str, float] = Field(default_factory=dict)
    dlq_pending: dict[str, list[FramePacket]] = Field(
        default_factory=dict,
        exclude=True,
        description="Packets preserved per table for post-mortem DLQ handling when spool/dispatch fails",
    )
    dlq_counts: dict[str, dict[str, int]] = Field(
        default_factory=dict,
        description="Per-table DLQ counts: {'rescued': int, 'remaining': int}",
    )

    def add_rows(self, *, table: str, rows: int) -> None:
        self.tables[table] = self.tables.get(table, 0) + rows


@dataclass(frozen=True, slots=True)
class ParseResult:
    """Result of parsing a response.

    Args:
        packets: List of extracted FramePackets containing data.
        next_jobs: List of subsequent FetchJobs to be executed.
    """

    packets: list[FramePacket]
    next_jobs: list[FetchJob]


__all__ = [
    "EngineConfig",
    "RetryConfig",
    "HttpMethod",
    "RequestAuth",
    "RequestSpec",
    "FetchJob",
    "FramePacket",
    "RunResult",
    "ParseResult",
]
