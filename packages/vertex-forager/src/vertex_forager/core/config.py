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
from vertex_forager.core.types import JSONValue
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
    """

    max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS
    base_backoff_s: float = DEFAULT_RETRY_BASE_BACKOFF_S
    max_backoff_s: float = DEFAULT_RETRY_MAX_BACKOFF_S


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

    Args:
        requests_per_minute: Maximum allowed requests per minute (must be positive).
        concurrency: Explicit concurrency limit (optional).
        retry: Retry configuration settings (default: RetryConfig()).
        flush_threshold_rows: Number of rows to buffer before flushing (default: 500,000).

    Raises:
        ValueError: If requests_per_minute is not positive.
    """

    # 1. Core Parameters
    requests_per_minute: int
    concurrency: int | None = None

    # 2. Retry Configuration
    retry: RetryConfig = Field(default_factory=RetryConfig)

    # 3. Advanced Tuning (Internal Defaults)
    flush_threshold_rows: int = FLUSH_THRESHOLD_ROWS

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


class RunResult(BaseModel):
    """Result summary for a pipeline run.

    Args:
        provider: Data provider name.
        tables: Dictionary mapping table names to row counts (default: empty dict).
        errors: List of error messages encountered (default: empty list).
    """

    provider: str
    tables: dict[str, int] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)

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
