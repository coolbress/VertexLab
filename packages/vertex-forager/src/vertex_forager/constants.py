"""Vertex Forager global constants.

Purpose:
- Centralize default values, thresholds, and protocol keys
- Ensure consistent configuration across providers (Sharadar, YFinance)
- Eliminate magic numbers; improve readability and maintainability

Notes:
- Units are included in names where applicable (e.g., *_S for seconds)
- Provider-specific constants live under providers/<name>/constants.py
"""

from __future__ import annotations

from typing import Final

DEFAULT_RATE_LIMIT: Final[int] = 60

DEFAULT_RETRY_MAX_ATTEMPTS: Final[int] = 3
DEFAULT_RETRY_BASE_BACKOFF_S: Final[float] = 1.0
DEFAULT_RETRY_MAX_BACKOFF_S: Final[float] = 30.0

RESERVED_PIPELINE_KEYS: Final[set[str]] = {
    "router",
    "dataset",
    "symbols",
    "writer",
    "mapper",
    "on_progress",
}

DATE_FMT: Final[str] = "%Y-%m-%d"
ISO8601_Z_SUFFIX: Final[str] = "+00:00"
DEFAULT_TIME_ZONE: Final[str] = "UTC"

HTTP_TIMEOUT_S: Final[float] = 30.0
HTTP_MAX_KEEPALIVE_CONNECTIONS: Final[int] = 100
HTTP_MAX_CONNECTIONS: Final[int] = 200
HTTP_USER_AGENT: Final[str] = "vertex-forager"

FLUSH_THRESHOLD_ROWS: Final[int] = 500_000

TICKERS_UNIT: Final[str] = "tickers"
PAGES_UNIT: Final[str] = "pages"

TRADING_DAYS_PER_YEAR: Final[int] = 252

PRIORITY_PAGINATION: Final[int] = 0
PRIORITY_NEW_JOB: Final[int] = 10
PRIORITY_SENTINEL: Final[int] = 999
FLUSH_THRESHOLD_INFINITE: Final[int] = 1_000_000_000
PROGRESS_LOG_CHUNK_ROWS: Final[int] = 100_000

# FlowController defaults
DEFAULT_AVG_LATENCY_S: Final[float] = 6.0
CONCURRENCY_MIN: Final[int] = 10
CONCURRENCY_MAX: Final[int] = 50
GRADIENT_QUEUE_SIZE_DEFAULT: Final[int] = 4
GRADIENT_SMOOTHING_DEFAULT: Final[float] = 0.2
GRADIENT_WINDOW_S: Final[float] = 60.0

# Queue sizing defaults for EngineConfig
QUEUE_TARGET_RAM_RATIO: Final[float] = 0.05
PACKET_SIZE_EST_BYTES: Final[int] = 5 * 1024 * 1024
QUEUE_MIN: Final[int] = 100
QUEUE_MAX: Final[int] = 2000
QUEUE_DEFAULT: Final[int] = 500

# Writer (DuckDB) defaults
WRITER_DUCKDB_MAX_WORKERS: Final[int] = 1
WAL_AUTOCHECKPOINT_LIMIT: Final[str] = "1GB"
