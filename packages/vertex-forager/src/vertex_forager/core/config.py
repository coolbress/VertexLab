from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any

import polars as pl
from pydantic import BaseModel, Field


class RetryConfig(BaseModel):
    """Retry configuration for HTTP requests.

    Args:
        max_attempts: Maximum number of retry attempts (default: 3).
        base_backoff_s: Initial backoff duration in seconds (default: 1.0).
        max_backoff_s: Maximum backoff duration in seconds (default: 30.0).
    """

    max_attempts: int = 3
    base_backoff_s: float = 1.0
    max_backoff_s: float = 30.0


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

    kind: str = "none"
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
    params: dict[str, str] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: dict[str, Any] | None = None
    data: bytes | None = None
    timeout_s: float = 30.0
    auth: RequestAuth = Field(default_factory=RequestAuth)


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
    context: dict[str, Any] = Field(default_factory=dict)


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
    context: dict[str, Any] = Field(default_factory=dict)

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
    flush_threshold_rows: int = 500_000  # ~40MB buffer

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
            # SC_PHYS_PAGES * SC_PAGE_SIZE = Total Memory in Bytes
            total_ram = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")

            # Target: 5% of RAM / Estimate: 5MB per packet
            target_buffer_bytes = total_ram * 0.05
            packet_size_est = 5 * 1024 * 1024

            calc_size = int(target_buffer_bytes / packet_size_est)

            # Bounds: Min 100, Max 2000
            return max(100, min(2000, calc_size))
        except (ValueError, AttributeError, ImportError):
            return 500

    def validate(self) -> None:
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
