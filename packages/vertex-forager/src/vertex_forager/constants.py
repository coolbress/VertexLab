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

HTTP_TIMEOUT_S: Final[float] = 60.0
HTTP_MAX_KEEPALIVE_CONNECTIONS: Final[int] = 100
HTTP_MAX_CONNECTIONS: Final[int] = 200
HTTP_USER_AGENT: Final[str] = "vertex-forager"

FLUSH_THRESHOLD_ROWS: Final[int] = 500_000
