# vertex-forager

Provider-agnostic data collection for financial markets. Centralized transport, schema‑aware normalization with Polars, and resilient writing with DLQ controls.

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
[![CI](https://github.com/coolbress/vertex-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/coolbress/vertex-lab/actions)
[![Docs](https://img.shields.io/badge/docs-MkDocs%20Material-blueviolet)](https://coolbress.github.io/vertex-lab/)

Status: Alpha • Python 3.10+ • License: Apache-2.0

## Features

- Unified `create_client(...)` API for YFinance and Sharadar
- Polars-first data handling with optional DuckDB persistence
- Built-in retry, flow control, and DLQ-backed write recovery

## Installation


```bash
# Using pip
pip install vertex-forager

# Using uv
uv pip install vertex-forager

# Optional extras
pip install "vertex-forager[notebook]"
pip install "vertex-forager[yfinance]"

# Install from GitHub release asset (specific tag)
pip install https://github.com/coolbress/VertexLab/releases/download/vertex-forager-v0.2.0/vertex_forager-0.2.0-py3-none-any.whl
```

## Quick Start

### YFinance

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
df = client.get_price_data(tickers=["AAPL", "MSFT"])
print(df)
```

Persist to DuckDB:

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
res = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb")
print(res)  # RunResult
```

### Sharadar

```python
import os
from vertex_forager import create_client

client = create_client(provider="sharadar", api_key=os.environ["SHARADAR_API_KEY"], rate_limit=120)
df = client.get_price_data(tickers=["AAPL", "MSFT"])
print(df)
```

## Configuration

- `create_client(...)`
  - required: `provider`, `api_key` (Sharadar), `rate_limit`
  - common runtime knobs: `concurrency`, `flush_threshold_rows`
  - state retention knobs: `checkpoint_retention_days`, `run_history_retention_days`
  - grouped config: `retry=RetryConfig(...)`, `throttle=AdaptiveThrottleConfig(...)`, `limits=HTTPConfig(...)`, `advanced=AdvancedConfig(...)`
- persistence path: `connect_db="duckdb://./forager.duckdb"`

## Documentation

- Tutorials
  - [Quickstart](docs/tutorials/quickstart.md)
- How‑to Guides
  - [Resume and recovery](docs/how-to/resume-and-recovery.md)
  - [Performance tuning](docs/how-to/performance-tuning.md)
  - [Data integrity controls](docs/how-to/data-integrity.md)
  - [Troubleshooting](docs/how-to/troubleshooting.md)
  - [CLI equivalents](docs/how-to/cli-equivalents.md)
- Reference
  - [Configuration](docs/reference/config.md)
  - [API Reference](docs/reference/api.md)

## Examples

- Scripts (uv):
  - `VF_TICKERS=AAPL uv run python packages/vertex-forager/examples/minimal_inmemory.py`
  - `VF_TICKERS="AAPL,MSFT" VF_DUCKDB_PATH=./forager.duckdb uv run python packages/vertex-forager/examples/advanced_duckdb_metrics.py`

## Versioning & Changelog

- Follows [Semantic Versioning 2.0.0](https://semver.org/) for public API.
- Changelog follows the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) convention.

## FAQ

- Do I need an API key?
  - Sharadar requires `SHARADAR_API_KEY`; YFinance does not.
- How do I change concurrency?
  - Pass `concurrency=...` to `create_client(...)`; it must be positive when specified.
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

Apache-2.0
Open Core: this repository contains the Apache-2.0 core; proprietary extensions are distributed separately.
