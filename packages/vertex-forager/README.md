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
  - common runtime knobs: `concurrency`, `flush_threshold_rows`, `writer_chunk_rows`, `dlq_enabled`, `metrics_enabled`
  - state retention knobs: `checkpoint_retention_days`, `run_history_retention_days`
  - grouped config: `retry=RetryConfig(...)`, `downshift=DownshiftConfig(...)`, `limits=HTTPConfig(...)`, `advanced=AdvancedConfig(...)`
- persistence path: `connect_db="duckdb://./forager.duckdb"`

## Documentation

- Tutorials
  - [Quickstart](docs/tutorials/quickstart.md)
- How‑to Guides
  - [Resume and recovery](docs/how-to/resume-and-recovery.md)
  - [Operate with DLQ disabled](docs/how-to/dlq-disabled.md)
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

## Server-side HTTP Status Retry

- Configurable retries for specific HTTP status codes (default: 429, 503).
- Exponential backoff with Full Jitter to reduce thundering herd; transport errors continue to retry.
- Configuration:
  - RetryConfig.enable_http_status_retry: bool (default True)
  - RetryConfig.retry_status_codes: tuple[int, ...] (default (429, 503))
- Structured logs include retry attempt metadata when enabled.

### Jitter and Opt-in Status Codes

- Backoff uses Full Jitter: sleep is drawn uniformly from [0, min(max_backoff_s, base_backoff_s * 2^(attempt-1))].
- Defaults are conservative. To broaden server error retries when appropriate:
  - `RetryConfig(retry_status_codes=(429, 503, 500, 502, 504))`
  - Important: Enable broader server error retries ONLY for idempotent operations.
    Non-idempotent requests (e.g., POST/PUT without idempotency keys) can cause duplicate side effects.
    Use idempotency keys or ensure upstream idempotent semantics before opting in.

### Per-request Idempotency Flag

- Each RequestSpec now supports `idempotent: bool` (default `True`).
- When `idempotent=False`, the retry controller performs a single attempt (no retry), even if transport/status rules match.
- Example:

```python
from vertex_forager.core.config import RequestSpec

# Non-idempotent request — do not retry
spec = RequestSpec(url="https://api.example.com/submit", method="POST", json_body={"x": 1}, idempotent=False)
```

## License

Apache-2.0
Open Core: this repository contains the Apache-2.0 core; proprietary extensions are distributed separately.
