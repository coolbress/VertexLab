from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

import polars as pl
import psutil
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from vertex_forager.constants import (
    DEFAULT_RETRY_BASE_BACKOFF_S,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_RETRY_MAX_BACKOFF_S,
    DLQ_TMP_RETENTION_S,
    FLUSH_THRESHOLD_ROWS,
    HTTP_TIMEOUT_S,
    PACKET_SIZE_EST_BYTES,
    QUEUE_DEFAULT,
    QUEUE_MAX,
    QUEUE_MIN,
    QUEUE_TARGET_RAM_RATIO,
)
from vertex_forager.core.contracts import TracerProtocol  # Pydantic v2: requires runtime resolution
from vertex_forager.core.errors import RunError, VertexForagerError
from vertex_forager.core.types import JSONValue  # Pydantic v2: used in field types at runtime

_RUNTIME_TYPE_REFERENCES = (Mapping, date, datetime, pl.DataFrame, TracerProtocol, JSONValue)


class RetryConfig(BaseModel):
    """Retry configuration for HTTP requests.

    Attributes:
        max_attempts (int): Maximum number of retry attempts (default: 3).
        base_backoff_s (float): Initial backoff duration in seconds (default: 1.0).
        max_backoff_s (float): Maximum backoff duration in seconds (default: 30.0).
        backoff_mode (Literal["full_jitter", "equal"]): Backoff distribution strategy (default: "full_jitter").
        retry_status_codes (tuple[int, ...]): Tuple of HTTP status codes to trigger retries (default: (429, 503)).

    Notes:
        - Two backoff modes are supported:
          - "full_jitter" (default): sleep is drawn uniformly from
            [0, min(max_backoff_s, base_backoff_s * 2^(attempt-1))].
          - "equal": sleep is drawn from [cap/2, cap] where cap = min(max_backoff_s, base_backoff_s * 2^(attempt-1)).
        - Retry-After response headers (integer seconds) take priority over backoff when present.
        - Defaults are conservative: retries on 429 (Too Many Requests) and 503 (Service Unavailable).
        - Opt-in to broader server errors (e.g., 500, 502, 504) ONLY when requests are idempotent.
          Non-idempotent operations (e.g., POST/PUT without idempotency keys) may cause duplicate side effects.
          Use idempotency keys or upstream idempotent semantics before enabling broader codes.
    """

    max_attempts: int = Field(default=DEFAULT_RETRY_MAX_ATTEMPTS, ge=1)
    base_backoff_s: float = Field(default=DEFAULT_RETRY_BASE_BACKOFF_S, ge=0.0)
    max_backoff_s: float = Field(default=DEFAULT_RETRY_MAX_BACKOFF_S, ge=0.0)
    backoff_mode: Literal["full_jitter", "equal"] = "full_jitter"
    retry_status_codes: tuple[int, ...] = (429, 503)

    @model_validator(mode="before")
    @classmethod
    def _reject_removed_enable_http_status_retry(cls, data: Any) -> Any:
        if not isinstance(data, Mapping) or "enable_http_status_retry" not in data:
            return data
        raise ValueError(
            "RetryConfig.enable_http_status_retry has been removed; "
            "use retry_status_codes=() to disable HTTP status retries."
        )

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


class AdaptiveThrottleConfig(BaseModel):
    """Adaptive throttle policy for dynamic RPM adjustment based on error rate.

    Attributes:
        enabled: Enables adaptive throttle behavior.
        window_s: Sliding window in seconds used to evaluate recent error rate.
        error_rate_threshold: Error ratio that triggers throttle decrease.
        rpm_floor_ratio: Minimum RPM as a ratio of ceiling (0.0-1.0) maintained while throttled.
        recovery_factor: RPM added as a fraction of ceiling during healthy recovery (0.0-1.0).
        healthy_window_s: Healthy period required before recovering RPM upward.

    Notes:
        - Uses AIMD (Additive Increase/Multiplicative Decrease) pattern.
        - Multiplicative decrease: new_rpm = effective_rpm * 0.8 when error threshold exceeded.
        - Additive increase: new_rpm = effective_rpm + max(1, ceiling * recovery_factor) when healthy.
        - rpm_floor is resolved to an absolute value at init: floor = max(1, ceiling * rpm_floor_ratio).
    """

    enabled: bool = False
    window_s: int = Field(default=60, ge=1)
    error_rate_threshold: float = Field(default=0.2, ge=0.0, le=1.0)
    rpm_floor_ratio: float = Field(default=0.10, ge=0.0, le=1.0)
    recovery_factor: float = Field(default=0.05, ge=0.0, le=1.0)
    healthy_window_s: int = Field(default=60, ge=1)

    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    @classmethod
    def _reject_legacy_downshift_fields(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        legacy_keys = {"downshift_enabled", "downshift_window_s", "rpm_floor", "recovery_step"}
        if legacy_keys & data.keys():
            raise TypeError(
                "DownshiftConfig has been renamed to AdaptiveThrottleConfig. "
                "Use throttle=AdaptiveThrottleConfig(...) instead. "
                "rpm_floor has been replaced by rpm_floor_ratio, and recovery_step by recovery_factor."
            )
        return data


class HTTPConfig(BaseModel):
    """HTTP connection-pool settings exposed as a grouped public config.

    Attributes:
        max_connections: Maximum connection pool size.
        max_keepalive_connections: Maximum keep-alive pool size.
    """

    max_connections: int = Field(default=200, ge=1)
    max_keepalive_connections: int = Field(default=100, ge=1)

    model_config = {"extra": "forbid"}


class AdvancedConfig(BaseModel):
    """Advanced and transitional client settings.

    Attributes:
        tracer: Optional tracing adapter implementing ``start_span``.
        otel_enabled: Optional tracing toggle.
    """

    tracer: TracerProtocol | None = None
    otel_enabled: bool | None = None

    model_config = {"arbitrary_types_allowed": True, "extra": "forbid"}


class SchedulerConfig(BaseModel):
    """Scheduler controls for always-on DRR pagination fairness."""

    quantum: int = Field(default=3, gt=0)
    max_pending_per_symbol: int | None = Field(default=None, gt=0)
    backpressure_threshold: int | None = Field(default=None, gt=0)

    model_config = {"extra": "forbid"}


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

    Attributes:
        kind (str): Authentication type — ``'none'``, ``'bearer'``, ``'header'``, or ``'query'`` (default: ``'none'``).
        token (str | None): Authentication token string if applicable (default: None).
        header_name (str | None): Name of the header to inject the token into (default: None).
        query_param (str | None): Name of the query parameter to inject the token into (default: None).
    """

    kind: Literal["none", "bearer", "header", "query"] = "none"
    token: str | None = None
    header_name: str | None = None
    query_param: str | None = None


class RequestSpec(BaseModel):
    """HTTP request specification for a fetch job.

    Attributes:
        method (HttpMethod): HTTP method to use (default: ``HttpMethod.GET``).
        url (str): Target URL for the request.
        params (dict[str, JSONValue]): Query parameters as key-value pairs (default: empty dict).
        headers (dict[str, str]): HTTP headers as key-value pairs (default: empty dict).
        json_body (dict[str, JSONValue] | None): JSON payload for POST/PUT requests (default: None).
        data (bytes | None): Raw bytes payload for requests (default: None).
        timeout_s (float): Request timeout in seconds (default: 30.0).
        auth (RequestAuth): Authentication strategy to apply (default: ``RequestAuth()``).
        idempotent (bool): Whether the request is safe to retry without side effects.
            Defaults to True; set to False to disable automatic retries for
            non-idempotent operations.
    """

    method: HttpMethod = HttpMethod.GET
    url: str
    params: dict[str, JSONValue] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: dict[str, JSONValue] | None = None
    data: bytes | None = None
    timeout_s: float = HTTP_TIMEOUT_S
    auth: RequestAuth = Field(default_factory=RequestAuth)
    idempotent: bool = True

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

    Attributes:
        provider (str): Data provider name (e.g., ``'sharadar'``).
        dataset (str): Dataset identifier (e.g., ``'SEP'``, ``'SF1'``).
        symbol (str | None): Target symbol or ticker if applicable (default: None).
        spec (RequestSpec): HTTP request specification details.
        context (Mapping[str, JSONValue]): Additional context for job execution and tracing (default: empty dict).
    """

    provider: str
    dataset: str
    symbol: str | None = None
    spec: RequestSpec
    context: Mapping[str, JSONValue] = Field(default_factory=dict)


class FramePacket(BaseModel):
    """Polars frame packet passed from provider to sink.

    Attributes:
        provider (str): Data provider name.
        table (str): Target table name for storage.
        frame (pl.DataFrame): Polars DataFrame containing the data.
        observed_at (datetime): Timestamp when the data was observed/fetched.
        partition_date (date | None): Optional date for partitioning logic (default: None).
        context (Mapping[str, JSONValue]): Metadata context passed along with the data (default: empty dict).
    """

    provider: str
    table: str
    frame: pl.DataFrame
    observed_at: datetime
    partition_date: date | None = None
    context: Mapping[str, JSONValue] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}


class ResolvedClientConfig(BaseModel):
    """Internal resolved snapshot of client configuration.

    This model is not part of the supported public package API. It exists so the
    runtime can carry a fully validated, default-applied view of the effective
    client settings after `create_client(...)` inputs have been normalized.

    Public callers should set configuration through `create_client(...)` and the
    grouped public config objects (`RetryConfig`, `AdaptiveThrottleConfig`, `HTTPConfig`,
    `AdvancedConfig`, `SchedulerConfig`) rather than constructing this model directly.
    """

    requests_per_minute: int = Field(..., gt=0)
    structured_logs: bool = False
    log_verbose: bool = False
    schedule: SchedulerConfig = Field(default_factory=SchedulerConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    adaptive_throttle: AdaptiveThrottleConfig = Field(default_factory=AdaptiveThrottleConfig)
    concurrency: int | None = Field(default=None, gt=0)
    flush_threshold_rows: int = FLUSH_THRESHOLD_ROWS
    http_timeout_s: float = Field(default=HTTP_TIMEOUT_S, gt=0)
    limits: HTTPConfig = Field(default_factory=HTTPConfig)
    advanced: AdvancedConfig = Field(default_factory=AdvancedConfig)
    checkpoint_retention_days: int = Field(default=7, ge=0)
    run_history_retention_days: int = Field(default=90, ge=0)

    model_config = {"arbitrary_types_allowed": True}

    @property
    def fetch_concurrency(self) -> int | None:
        return self.concurrency

    @property
    def adaptive_throttle_enabled(self) -> bool:
        return self.adaptive_throttle.enabled

    @property
    def adaptive_throttle_window_s(self) -> int:
        return self.adaptive_throttle.window_s

    @property
    def error_rate_threshold(self) -> float:
        return self.adaptive_throttle.error_rate_threshold

    @property
    def rpm_floor_ratio(self) -> float:
        return self.adaptive_throttle.rpm_floor_ratio

    @property
    def recovery_factor(self) -> float:
        return self.adaptive_throttle.recovery_factor

    @property
    def healthy_window_s(self) -> int:
        return self.adaptive_throttle.healthy_window_s

    @property
    def dlq_tmp_retention_s(self) -> int:
        return getattr(self.advanced, "dlq_tmp_retention_s", DLQ_TMP_RETENTION_S)

    @property
    def quantum(self) -> int:
        return self.schedule.quantum

    @property
    def max_pending_per_symbol(self) -> int | None:
        return self.schedule.max_pending_per_symbol

    @property
    def backpressure_threshold(self) -> int | None:
        return self.schedule.backpressure_threshold

    @property
    def tracer(self) -> TracerProtocol | None:
        return self.advanced.tracer

    @property
    def otel_enabled(self) -> bool | None:
        return self.advanced.otel_enabled

    @property
    def queue_max(self) -> int:
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
        if self.requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        if self.concurrency is not None and self.concurrency <= 0:
            raise ValueError("concurrency must be positive if specified")
        try:
            checkpoint_retention_days = (
                7 if self.checkpoint_retention_days is None else int(self.checkpoint_retention_days)
            )
        except (TypeError, ValueError) as e:
            raise VertexForagerError(f"checkpoint_retention_days must be an integer >= 0: {e}") from e
        self.checkpoint_retention_days = max(0, checkpoint_retention_days)
        try:
            run_history_retention_days = (
                90 if self.run_history_retention_days is None else int(self.run_history_retention_days)
            )
        except (TypeError, ValueError) as e:
            raise VertexForagerError(f"run_history_retention_days must be an integer >= 0: {e}") from e
        self.run_history_retention_days = max(0, run_history_retention_days)


class RunResult(BaseModel):
    """Result summary for a pipeline run.

    Attributes:
        provider (str): Data provider name.
        run_id (str | None): Unique identifier for the run (default: None).
        dataset (str | None): Dataset name for the run (default: None).
        started_at (float | None): Timestamp when the run started (default: None).
        finished_at (float | None): Timestamp when the run finished (default: None).
        duration_s (float | None): Duration of the run in seconds (default: None).
        coverage_pct (float | None): Coverage percentage for the run (default: None).
        tables (dict[str, int]): Dictionary mapping table names to row counts (default: empty dict).
        errors (list[RunError]): List of structured error information (default: empty list).
        dlq_pending (dict[str, list[FramePacket]]): Packets preserved for post-mortem/dead-letter
            processing when DLQ spool/dispatch fails. Items are appended by writer/rescue
            logic upon spool errors and can be consumed by operator recovery flows.
        dlq_counts (dict[str, dict[str, int]]): Per-table counts for rescued and remaining packets
            when DLQ is disabled or spooling occurs. Always populated regardless of metrics settings.
        quality_violations (dict[str, int]): Dictionary mapping table names to quality violation counts
            (default: empty dict).
    """

    provider: str
    run_id: str | None = Field(default=None)
    dataset: str | None = Field(default=None)
    started_at: float | None = Field(default=None)
    finished_at: float | None = Field(default=None)
    duration_s: float | None = Field(default=None)
    coverage_pct: float | None = Field(default=None)
    tables: dict[str, int] = Field(default_factory=dict)
    errors: list[RunError] = Field(default_factory=list)
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
    quality_violations: dict[str, int] = Field(
        default_factory=dict,
        description="Per-table quality violation counts",
    )

    @field_validator("errors", mode="before")
    @classmethod
    def _coerce_string_errors(cls, v: Any) -> Any:
        if not isinstance(v, (list, tuple)):
            return v
        result: list[Any] = []
        for item in v:
            if isinstance(item, str):
                result.append(
                    RunError(
                        provider="",
                        dataset="",
                        symbol="",
                        exc_type="builtins.str",
                        message=item,
                        retryable=False,
                    )
                )
            else:
                result.append(item)
        return result

    def add_rows(self, *, table: str, rows: int) -> None:
        self.tables[table] = self.tables.get(table, 0) + rows

    def add_quality_violations(self, *, table: str, count: int) -> None:
        self.quality_violations[table] = self.quality_violations.get(table, 0) + count


class ProgressSnapshot(BaseModel):
    jobs_done: int
    jobs_total: int | None
    pct: float | None
    throughput_sym_per_s: float
    eta_s: float | None
    errors: int
    retries: int
    rows_written: int
    elapsed_s: float
    active_workers: int
    pending_jobs: int
    throttle_events: int
    dlq_spooled: int
    memory_mb: float
    cpu_pct: float
    finished: bool = False


@dataclass(frozen=True, slots=True)
class ParseResult:
    """Result of parsing a response.

    Attributes:
        packets (list[FramePacket]): List of extracted FramePackets containing data.
        next_jobs (list[FetchJob]): List of subsequent FetchJobs to be executed.
    """

    packets: list[FramePacket]
    next_jobs: list[FetchJob]


__all__ = [
    "AdaptiveThrottleConfig",
    "AdvancedConfig",
    "FetchJob",
    "FramePacket",
    "HTTPConfig",
    "HttpMethod",
    "ParseResult",
    "ProgressSnapshot",
    "RequestAuth",
    "RequestSpec",
    "RetryConfig",
    "RunResult",
    "SchedulerConfig",
]
