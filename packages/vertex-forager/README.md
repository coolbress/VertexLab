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

res = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb"])
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

## License

MIT
