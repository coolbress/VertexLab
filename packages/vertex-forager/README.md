# vertex-forager

Provider-agnostic data collection for financial markets. Centralized transport, scheme-based library fetchers, and structured normalization with Polars.

Status: Alpha • Python 3.10+ • License: MIT

## Table of Contents

- Features
- Installation
- Quick Start
- Providers
- Configuration
- Usage Patterns
- Examples
- FAQ
- Contributing
- License

## Features

- Provider-agnostic core with centralized HTTP transport and retry
- Scheme-based library fetchers (e.g., `yfinance://`) with safe invocation rules
- Structured logs and error accumulation with `RunResult`
- Polars-based normalization with schema registry and PK-aware writing
- Writer support: DuckDB (upsert/index), in-memory buffer

## Installation


```bash
# Using pip
pip install vertex-forager

# Using uv
uv pip install vertex-forager
```

## Quick Start

Use provider-specific clients directly (no manual router/writer setup).

### YFinance (library provider)

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
df = client.get_price_data(tickers=["AAPL", "MSFT"])
print(df)
```

Persist to DuckDB:

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
res = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb")
print(res)  # RunResult
```

### Sharadar (HTTP provider)

```python
import os
from vertex_forager import create_client

client = create_client(provider="sharadar", api_key=os.environ["SHARADAR_API_KEY"], rate_limit=120)
df = client.get_price_data(tickers=["AAPL", "MSFT"])
print(df)
```

Persist to DuckDB:

```python
import os
from vertex_forager import create_client

client = create_client(provider="sharadar", api_key=os.environ["SHARADAR_API_KEY"], rate_limit=120)
res = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb")
print(res)  # RunResult
```

## Providers

- Sharadar (Nasdaq Data Link): HTTP JSON; datasets include `price`, `daily`, `fundamental`, `actions`, `tickers`, `sp500`
- YFinance: library-backed data via `yfinance://` scheme; datasets include `info`, `price`, `dividends`, `splits`, `actions`, `calendar`, `news`

## Provider Client Examples


### YFinanceClient

```python

client = create_client(provider="yfinance", rate_limit=60)
df = client.get_price_data(tickers=["AAPL", "MSFT"])
print(df)

res = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb")
print(res)  # RunResult
```

### SharadarClient

```python
import os
from vertex_forager import create_client

client = create_client(provider="sharadar", api_key=os.environ["SHARADAR_API_KEY"], rate_limit=120)
df = client.get_price_data(tickers=["AAPL", "MSFT"])
print(df)

res = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb")
print(res)  # RunResult
```

## Configuration

- EngineConfig
  - `requests_per_minute`: positive integer (required)
  - `concurrency`: optional positive integer
  - `retry`: `{max_attempts, base_backoff_s, max_backoff_s}`
- Flow control
  - Global rate limiting via `FlowController` (automatic in BaseClient)
- Writers
  - DuckDB: `duckdb://./path/to/db.duckdb` (unique index created if PK known)
  - In-memory: `memory://`

## Observability

- Metrics (enabled via `EngineConfig.metrics_enabled=True`)
  - Counters:
    - `rows_written_total`
    - `writer_flushes`
    - `errors_total`
    - `dlq_spooled_files_total`, `dlq_rescued_total`, `dlq_remaining_total`
  - Histograms:
    - `fetch_duration_s`, `parse_duration_s`, `http_duration_s`, `writer_flush_duration_s`
    - Per table: `writer_flush_duration_s.{table}`, `writer_rows.{table}`
  - Summary (selected percentiles):
    - Global p95/p99 for durations
    - Per table p50/p95/p99 for `writer_flush_duration_s.{table}` and `writer_rows.{table}`
  - Queue snapshots:
    - `req_q_len_after_producer`, `req_q_len_after_req_join`, `pkt_q_len_after_producer`, `pkt_q_len_after_pkt_join`
- Optional spans (no hard dependency)
  - Set env `VF_OTEL_ENABLED=1` and provide `EngineConfig.tracer` with a `start_span(name, attributes=...)` method to receive spans for `pipeline`, `fetch`, `parse`, and `write_flush`.

## Usage Patterns

- Transport decoupling: Routers are transport-agnostic; normalize after decoding
- Scheme-based fetchers: non-HTTP library calls routed via plugin registry
- Error handling:
  - Fetch/log errors recorded in `RunResult.errors`
  - Writer validation errors surfaced (PK missing/nulls)
  - Retry exhaustion categorized as fetch-specific error

## Examples

- Notebooks:
  - packages/vertex-forager/examples/sharadar.ipynb
  - packages/vertex-forager/examples/yfinance_examples.ipynb
- Verification scripts:
  - tests/verification/verify_duckdb_upsert.py
  - tests/verification/verify_pipeline_perf.py

## FAQ

- Do I need an API key?
  - Sharadar requires `SHARADAR_API_KEY`; YFinance does not.
- How do I change concurrency?
  - Set `EngineConfig(concurrency=...)`; must be positive when specified.
- Where are schemas defined?
  - See `vertex_forager/schema/registry.py` and provider-specific `schema.py`.

## Public API


```python
from vertex_forager import (
  SharadarClient, YFinanceClient,
  create_client, create_router,
  FetchError, ValidationError, WriterError,
)
```

## Contributing

- Use uv for environment management; run ruff/mypy/pytest before PRs.
- Keep provider-specific logic isolated in provider modules.

## Server-side HTTP Status Retry

- Configurable retries for specific HTTP status codes (default: 429, 503).
- Exponential backoff with Full Jitter to reduce thundering herd; transport errors continue to retry.
- Configuration:
  - EngineConfig.retry.enable_http_status_retry: bool (default True)
  - EngineConfig.retry.retry_status_codes: tuple[int, ...] (default (429, 503))
- Structured logs include retry attempt metadata when enabled.

### Jitter and Opt-in Status Codes

- Backoff uses Full Jitter: sleep is drawn uniformly from [0, min(max_backoff_s, base_backoff_s * 2^(attempt-1))].
- Defaults are conservative. To broaden server error retries when appropriate:
  - EngineConfig.retry.retry_status_codes = (429, 503, 500, 502, 504)
  - Important: Enable broader server error retries ONLY for idempotent operations.
    Non-idempotent requests (e.g., POST/PUT without idempotency keys) can cause duplicate side effects.
    Use idempotency keys or ensure upstream idempotent semantics before opting in.

## License

MIT
