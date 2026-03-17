# vertex-forager

Provider-agnostic data collection for financial markets. Centralized transport, schema‑aware normalization with Polars, and resilient writing with DLQ controls.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
[![CI](https://github.com/coolbress/vertex-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/coolbress/vertex-lab/actions)
[![Docs](https://img.shields.io/badge/docs-MkDocs%20Material-blueviolet)](https://coolbress.github.io/vertex-lab/)

Status: Alpha • Python 3.10+ • License: MIT

## Table of Contents

- Features
- Installation
- Quick Start
- Providers
- Configuration
- Observability
- Usage Patterns
- Documentation
- Versioning & Changelog
- Examples
- FAQ
- Contributing
- License

## Features

- Transport and flow control
  - Central HTTP executor with retry (Full Jitter), GCRA pacing, gradient concurrency control
  - Adaptive RPM downshift/recovery with floor/threshold guards
- Schema‑aware pipeline
  - Polars normalization with central schema registry and PK validation
  - Flexible schema opt‑in for evolving library providers
- Resilient writing
  - Chunked flush to bound memory peak; per‑chunk accounting
  - DLQ spool with fsync and atomic replace; `dlq_enabled` toggle to disable spooling when required
  - RunResult summaries and per‑table DLQ counts regardless of metrics settings
- Writers
  - DuckDB (unique index if PK known)
  - In‑memory buffer

## Installation


```bash
# Using pip
pip install vertex-forager

# Using uv
uv pip install vertex-forager

# Install from GitHub release asset (specific tag)
pip install https://github.com/coolbress/VertexLab/releases/download/vertex-forager-v0.2.0/vertex_forager-0.2.0-py3-none-any.whl
```

## Quick Start

Use provider‑specific clients directly (no manual router/writer setup).

### Architecture (high‑level)

```mermaid
flowchart TD
  A[Router Jobs] --> B[Throttle (GCRA) + Concurrency (Gradient)]
  B --> C[HTTP / Library Fetch with Retry]
  C --> D[Parse -> FramePackets]
  D --> E[Normalize (Schemas, PK)]
  E --> F{Flush threshold reached?}
  F -- No --> E
  F -- Yes --> G[Merge Frames, PK Checks]
  G --> H{Write Chunk}
  H -- Success --> I[Update RunResult & Metrics]
  H -- Failure --> J[Per-packet Rescue]
  J -- Partial Success --> K[DLQ Spool Failed Packets]
  J -- All Fail --> K
  K --> L[Operator Recovery CLI]
  L --> H
```

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
  - `flush_threshold_rows`: buffer flush threshold (rows)
  - `writer_chunk_rows`: per‑chunk rows for streaming write (>= 10_000 when set)
  - `dlq_enabled`: enable/disable DLQ spooling (default True)
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
    - `dlq_spooled_files_total`, `dlq_rescued_total`, `dlq_remaining_total`, `dlq_spool_failed_total`
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

## Documentation

- Tutorials
  - [Quickstart](docs/tutorials/quickstart.md)
- How‑to Guides
  - [Operate with DLQ disabled](docs/how-to/dlq-disabled.md)
  - [Tune chunked flush thresholds](docs/how-to/chunked-flush.md)
  - [Troubleshooting](docs/how-to/troubleshooting.md)
  - [CLI equivalents](docs/how-to/cli-equivalents.md)
- Reference
  - [EngineConfig](docs/reference/config.md)
  - [Metrics](docs/reference/metrics.md)
  - [Constants](docs/reference/constants.md)
  - [API Reference](docs/reference/api.md)
- Explanation
  - [Pipeline architecture and design](docs/explanation/architecture.md)
  - [Router & Client](docs/explanation/router-client.md)
  - [Data storage & DLQ](docs/explanation/data-storage-flow.md)
  - [Writer security](docs/explanation/writer-security.md)
  - [Writer fan‑out roadmap](docs/explanation/writer-fanout-roadmap.md)

## Examples

- Scripts (uv):
  - Minimal in‑memory (single ticker):
    - `VF_TICKERS=AAPL uv run python packages/vertex-forager/examples/minimal_inmemory.py`
      - `VF_TICKERS`: required (comma‑separated tickers; single ticker shown)
  - Advanced (multi‑ticker, DuckDB, metrics, chunked flush):
    - `VF_TICKERS="AAPL,MSFT" VF_DUCKDB_PATH=./forager.duckdb uv run python packages/vertex-forager/examples/advanced_duckdb_metrics.py`
      - `VF_TICKERS`: required (multi‑ticker demo)
      - `VF_DUCKDB_PATH`: optional (defaults to `./forager.duckdb`)
- CLI equivalents:
  - See [CLI Equivalents](docs/how-to/cli-equivalents.md)
- Notebooks (optional):
  - packages/vertex-forager/examples/sharadar.ipynb
  - packages/vertex-forager/examples/yfinance_examples.ipynb

## Versioning & Changelog

- Follows [Semantic Versioning 2.0.0](https://semver.org/) for public API.
- Changelog follows the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) convention.

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
