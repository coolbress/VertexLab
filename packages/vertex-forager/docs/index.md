# vertex-forager

[![PyPI](https://img.shields.io/pypi/v/vertex-forager)](https://pypi.org/project/vertex-forager/)
[![CI](https://github.com/coolbress/VertexLab/actions/workflows/ci.yml/badge.svg)](https://github.com/coolbress/VertexLab/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/coolbress/VertexLab/blob/main/LICENSE)

Provider-agnostic financial data ingestion with resilient writes, schema-aware normalization, and operational controls for production-oriented pipelines.

## Why vertex-forager

- Fetch data through a unified client layer across HTTP and library-backed providers
- Normalize frames with Polars and schema-aware validation before persistence
- Persist safely with DuckDB and DLQ-backed recovery controls
- Inspect configuration, metrics, and API behavior through dedicated reference docs

## Installation

```bash
pip install vertex-forager
pip install "vertex-forager[yfinance]"
pip install "vertex-forager[notebook]"
```

See [Installation](get-started/installation.md) for installation options, extras, and repository-based workflows.

## Quick Example

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
result = client.get_price_data(
    tickers=["AAPL", "MSFT"],
    connect_db="duckdb://./forager.duckdb",
)
print(result)
```

## Start Here

- Install the package: [Installation](get-started/installation.md)
- Run your first workflow: [Quickstart](tutorials/quickstart.md)
- Learn the test workflow: [Testing Guide](get-started/testing.md)
- Review common operational tasks: [How-to Guides](how-to/troubleshooting.md)

## Documentation Map

- Tutorials: [Quickstart](tutorials/quickstart.md)
- How-to Guides:
  - [Migrate between releases](how-to/migration.md)
  - [Operate with DLQ disabled](how-to/dlq-disabled.md)
  - [Tune chunked flush thresholds](how-to/chunked-flush.md)
  - [Performance tuning](how-to/performance-tuning.md)
  - [Troubleshooting](how-to/troubleshooting.md)
  - [CLI equivalents](how-to/cli-equivalents.md)
  - [Provider extension](how-to/provider-extension.md)
  - [Provider plugin](how-to/provider-plugin.md)
  - [Data integrity controls](how-to/data-integrity.md)
- Reference:
  - [API Reference](reference/api.md)
  - [Module Index](reference/modules.md)
  - [EngineConfig](reference/config.md)
  - [Metrics](reference/metrics.md)
  - [Constants](reference/constants.md)
  - [Schema](reference/schema.md)
- Explanation:
  - [Pipeline architecture](explanation/architecture.md)
  - [Router & Client architecture](explanation/router-client.md)
  - [Data storage flow & DLQ](explanation/data-storage-flow.md)
  - [Core error policy](explanation/error-policy.md)
  - [Writer security](explanation/writer-security.md)
  - [Writer fan-out roadmap](explanation/writer-fanout-roadmap.md)
  - [Writer upsert behavior](explanation/writer-upsert-behavior.md)

## Examples And Project Links

- [YFinance notebook](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/examples/yfinance_examples.ipynb)
- [Sharadar notebook](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/examples/sharadar.ipynb)
- [Changelog](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/CHANGELOG.md)
- [Contributing](https://coolbress.github.io/VertexLab/contributing/)
