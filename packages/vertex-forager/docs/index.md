# vertex-forager

[![Distribution](https://img.shields.io/badge/distribution-GitHub%20Releases-blue)](https://github.com/coolbress/VertexLab/releases)
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
pip install "vertex-forager @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

vertex-forager is currently distributed from GitHub rather than PyPI. See [Installation](installation.md) for install variants, extras, and repository-based workflows.

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

- Install the package: [Installation](installation.md)
- Run your first workflow: [Quickstart](tutorials/quickstart.md)
- Learn the test workflow: [Testing Guide](how-to/testing.md)
- Review common operational tasks: [How-to Guides](how-to/troubleshooting.md)

## Examples And Project Links

- [YFinance notebook](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/examples/yfinance_examples.ipynb)
- [Sharadar notebook](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/examples/sharadar.ipynb)
- [Changelog](changelog.md)
- [Contributing](https://coolbress.github.io/VertexLab/contributing/)
