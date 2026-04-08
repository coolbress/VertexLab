# vertex-forager

Provider-agnostic data collection for financial markets. Centralized transport, schema‑aware normalization with Polars, and resilient writing with DLQ controls.

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
[![CI](https://github.com/coolbress/vertex-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/coolbress/vertex-lab/actions)
[![Docs](https://img.shields.io/badge/docs-MkDocs%20Material-blueviolet)](https://coolbress.github.io/VertexLab/vertex-forager/)

Status: Alpha • Python 3.10+ • License: Apache-2.0

## Features

- Unified `create_client(...)` API for YFinance and Sharadar
- Polars-first data handling with optional DuckDB persistence
- Built-in retry, flow control, and DLQ-backed write recovery
- Canonical persisted table names and discriminator-safe shared-table storage

## Installation

```bash
pip install "vertex-forager[yfinance] @ git+https://github.com/coolbress/VertexLab.git#subdirectory=packages/vertex-forager"
```

This README tracks the current branch documentation, so the install example intentionally targets the repository package path without pinning an older tag.

## Quick Start

### YFinance

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
result = client.get_price_data(tickers=["AAPL", "MSFT"])
print(result.data)
```

Persist to DuckDB:

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
result = client.get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb://./forager.duckdb")
print(result.tables)  # RunResult
```

### Sharadar

```python
import os
from vertex_forager import create_client

client = create_client(provider="sharadar", api_key=os.environ["SHARADAR_API_KEY"], rate_limit=120)
result = client.get_price_data(tickers=["AAPL", "MSFT"])
print(result.data)
```

`get_price_data(...)` returns a `RunResult`. In in-memory mode, the collected DataFrame is available under `result.data`.

## Configuration

- `create_client(...)`
  - required: `provider`
  - Sharadar also requires `api_key` and `rate_limit`
  - common public knobs: `quality_check`, `concurrency`, `schedule`
  - grouped config: `retry=RetryConfig(...)`, `throttle=AdaptiveThrottleConfig(...)`, `limits=HTTPConfig(...)`, `storage=StorageConfig(...)`
- persistence path: `connect_db="duckdb://./forager.duckdb"`

## Storage contract

- Sharadar persisted table names follow collect-method vocabulary: `sharadar_price`, `sharadar_fundamental`, `sharadar_insider`, `sharadar_institutional`
- Shared-table YFinance flows persist explicit discriminators:
  - `yfinance_financials.statement_kind`
  - `yfinance_holders.holder_type`
- See [Migration Guide](docs/how-to/migration.md) before upgrading existing DuckDB files

## Documentation

- Tutorials
  - [Quickstart](docs/tutorials/quickstart.md)
- How‑to Guides
  - [Configure a client](docs/how-to/configure-a-client.md)
  - [Collect data](docs/how-to/collect-data.md)
  - [Manage local state](docs/how-to/manage-local-state.md)
  - [Resume interrupted run](docs/how-to/resume-interrupted-run.md)
  - [Performance tuning](docs/how-to/performance-tuning.md)
  - [Troubleshooting](docs/how-to/troubleshooting.md)
- Reference
  - [CLI Reference](docs/reference/cli.md)
  - [Providers](docs/reference/providers.md)
  - [StateManager](docs/reference/statemanager.md)
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
- How do I inspect or replay local state?
  - Use `StateManager()` or the CLI `runs`, `dlq`, and `checkpoints` commands.
- Where are schemas defined?
  - See `vertex_forager/schema/registry.py` and provider-specific `schema.py`.

## Public API


```python
from vertex_forager import (
  create_client, StateManager,
  SharadarClient, YFinanceClient,
  VertexForagerError, InputError,
  FetchError, ValidationError, WriterError,
)
```

## Contributing

- Use uv for environment management; run ruff/mypy/pytest before PRs.
- Keep provider-specific logic isolated in provider modules.

## License

Apache-2.0
Open Core: this repository contains the Apache-2.0 core; proprietary extensions are distributed separately.
