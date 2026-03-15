# Vertex Forager Constants Overview

This document summarizes centralized constants across the codebase. The goal is to eliminate magic numbers and provide a single source of truth for operational defaults and provider-specific mappings.

## Global (`vertex_forager/constants.py`)

- HTTP_TIMEOUT_S, HTTP_MAX_CONNECTIONS, HTTP_MAX_KEEPALIVE_CONNECTIONS, HTTP_USER_AGENT
- DEFAULT_RATE_LIMIT, DEFAULT_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_BASE_BACKOFF_S, DEFAULT_RETRY_MAX_BACKOFF_S
- RESERVED_PIPELINE_KEYS
- DATE_FMT, ISO8601_Z_SUFFIX, DEFAULT_TIME_ZONE
- FLUSH_THRESHOLD_ROWS, FLUSH_THRESHOLD_INFINITE
- PRIORITY_PAGINATION, PRIORITY_NEW_JOB, PRIORITY_SENTINEL
- PROGRESS_LOG_CHUNK_ROWS
- TICKERS_UNIT, PAGES_UNIT
- TRADING_DAYS_PER_YEAR
- FlowController defaults: DEFAULT_AVG_LATENCY_S, CONCURRENCY_MIN, CONCURRENCY_MAX, GRADIENT_QUEUE_SIZE_DEFAULT, GRADIENT_SMOOTHING_DEFAULT, GRADIENT_WINDOW_S
- Queue sizing: QUEUE_TARGET_RAM_RATIO, PACKET_SIZE_EST_BYTES, QUEUE_MIN, QUEUE_MAX, QUEUE_DEFAULT
- Writer(DuckDB): WRITER_DUCKDB_MAX_WORKERS, WAL_AUTOCHECKPOINT_LIMIT

## YFinance (`providers/yfinance/constants.py`)

- SIZE_MAP
- PRICE_BATCH_SIZE, PRICE_BATCH_MAX, THREADS_THRESHOLD
- PRICE_BATCH_SIZE_KEY
- DATASET_ENDPOINT (dataset → yfinance API attribute/endpoint)
- DATE_FILTER_COL
- Price defaults/keys: INTERVAL_KEY, START_KEY, END_KEY, PERIOD_KEY, AUTO_ADJUST_KEY, PREPOST_KEY, DEFAULT_INTERVAL, DEFAULT_PRICE_PERIOD, DEFAULT_AUTO_ADJUST, DEFAULT_PREPOST

## Sharadar (`providers/sharadar/constants.py`)

- MAX_ROWS_PER_REQUEST, DEFAULT_BATCH_SIZE, MIN_BATCH_SIZE
- TRADING_DAYS_RATIO, QUARTERLY_DAYS_RATIO
- Pagination keys: PAGINATION_META_KEY, PAGINATION_CURSOR_PARAM, MAX_PAGES
- DATASET_ENDPOINT (dataset → Nasdaq Data Link endpoint)
- DATE_FILTER_COL
- INTERNAL_COLS
- Request keys: QOPTS_PER_PAGE, QOPTS_COLUMNS, API_KEY_QUERY_PARAM
- Estimates: BYTES_PER_TICKER_METADATA, BYTES_PER_TICKER_FULL, ESTIMATED_TOTAL_TICKERS

## Key Naming Rules (Comparison)

| Category | Sharadar | YFinance | Notes |
|---|---|---|---|
| Date Filter Key | DATE_FILTER_COL | DATE_FILTER_COL | Values map to schema date columns |
| Pagination Per-Page | qopts.per_page | N/A | Sharadar Datatables pagination |
| Column Selection | qopts.columns | N/A | Sharadar Datatables requested columns |
| Auth Param | api_key | N/A | Sharadar uses query param auth |
| Price Interval | N/A | interval | Default 1d |
| Range Start | N/A | start | YYYY-MM-DD |
| Range End | N/A | end | YYYY-MM-DD |
| Period | N/A | period | Default max when no start_date |
| Auto Adjust | N/A | auto_adjust | Default False |
| Pre/Post | N/A | prepost | Default False |

## Units/Scope/Impact Notes

- HTTP_TIMEOUT_S (seconds): RequestSpec.default timeout; lowering may increase failures, raising may stall retries.
- DEFAULT_RATE_LIMIT (rpm): FlowController rate; affects overall throughput and API throttling risk.
- FLUSH_THRESHOLD_ROWS (rows): Pipeline buffering threshold; high values reduce merges but increase memory.
- TRADING_DAYS_PER_YEAR (days): Batch estimation heuristic; used for Sharadar range sizing.
- WAL_AUTOCHECKPOINT_LIMIT (bytes string): DuckDB WAL auto-checkpoint; larger limits reduce checkpoint overhead during bursts.
- DEFAULT_TIME_ZONE (tz): All timestamps stored consistently; switching impacts downstream parsing/queries.

## DIP Rules & Boundaries (Summary)

- Core → Abstractions: Depend on `IRouter`, `IWriter`, `IMapper` Protocols, not concrete classes.
- Providers → Implementations: Routers and library fetchers live under `vertex_forager/providers/*`.
- Routers Utilities: `vertex_forager/routers/transforms.py` and `routers/errors.py` are provider-agnostic helpers scoped to the routers layer.
- Factories/Registries: Use `create_router` and writer registries to inject implementations; avoid direct imports of concretes in core.

## Logging Prefix Rules

- Core HTTP (`vertex_forager.core.http`):
  - Error logs include `provider`, `status` (if available), and redacted messages.
  - Library fetch branch logs: include `scheme`, `dataset`, `symbol`, and exception type for traceability.
- Writers (`vertex_forager.writers.constants`):
  - Prefixes: `WRITER`, `DUCKDB`.
  - Correlation summary logs include counts and sample IDs for `trace_id`/`request_id`.
