from __future__ import annotations

from typing import Final

ROUTER_LOG_PREFIX: Final[str] = "ROUTER"
CLIENT_LOG_PREFIX: Final[str] = "CLIENT"

# Router common
LOG_META_MISSING_COLS: Final[str] = "{prefix}: Metadata missing required columns {required}. Smart batching disabled."
LOG_META_PROCESSED: Final[str] = "{prefix}: Processed metadata for {count} tickers."
LOG_META_PROCESS_FAIL: Final[str] = "{prefix}: Failed to process ticker metadata: {error}. Smart batching disabled."
LOG_AUTH_TOKEN: Final[str] = "{prefix}: Auth token={masked}"
LOG_UNSUPPORTED_DATASET: Final[str] = "{prefix}: Unsupported dataset: {dataset}"
LOG_PRICE_BATCH_PARSE_FAIL: Final[str] = "{prefix}: Failed to parse price_batch_size='{value}', using default={default}"
LOG_INVALID_RATE_LIMIT: Final[str] = "{prefix}: Invalid rate_limit value '{value}'; falling back to {fallback} rpm."
LOG_PARSE_FAILED_JOB: Final[str] = "{prefix}: Parse failed for job {job_id}"
LOG_PARSE_UNEXPECTED_ERROR: Final[str] = "{prefix}: Unexpected error in parse for job {job_id}"
LOG_POLARS_CONVERT_FAIL: Final[str] = "{prefix}: Failed to convert data to Polars: {error}"
LOG_POLARS_CONVERT_UNEXPECTED: Final[str] = "{prefix}: Unexpected failure converting data to Polars"

# Client common
LOG_META_CACHE_MISS: Final[str] = "{prefix}: Metadata cache miss. Fetching ticker metadata first..."
LOG_META_CACHED_COUNT: Final[str] = "{prefix}: Metadata cached: {count} tickers"
LOG_META_PREFETCH_FAIL: Final[str] = "{prefix}: Failed to prefetch metadata: {error}. Smart batching will be disabled."
LOG_RATE_LIMIT_INVALID_INT: Final[str] = "{prefix}: rate_limit {value} is invalid; falling back to default."
LOG_RATE_LIMIT_EXCEEDS_DEFAULT: Final[str] = "{prefix}: rate_limit {value} exceeds default; API may throttle."
LOG_RATE_LIMIT_INVALID_TYPE: Final[str] = "{prefix}: rate_limit '{value}' is invalid; falling back to default."

# Router decisions (pagination/batching/build)
LOG_PAGINATION_START: Final[str] = "{prefix}: Starting pagination for dataset={dataset} per_page={per_page}"
LOG_BATCH_FORCE_SINGLE: Final[str] = "{prefix}: Forcing single-symbol job for {symbol} (est_rows={est_rows} > max_rows={max_rows})"
LOG_BATCH_FLUSH: Final[str] = "{prefix}: Flushing batch size={size} rows={rows}"
LOG_BATCH_ADD: Final[str] = "{prefix}: Added {symbol} to batch (est_rows={est_rows}) current_rows={current_rows}"
LOG_HEURISTIC_BATCH_SIZE: Final[str] = "{prefix}: Heuristic batch_size for dataset={dataset} is {batch_size}"
LOG_BUILD_JOB: Final[str] = "{prefix}: Built job dataset={dataset} symbols={symbols}"
LOG_PRICE_PARAMS: Final[str] = "{prefix}: Price params interval={interval} start={start} end={end} period={period}"
