# vertex-forager

[![Distribution](https://img.shields.io/badge/distribution-GitHub%20Releases-blue)](https://github.com/coolbress/VertexLab/releases)
[![CI](https://github.com/coolbress/VertexLab/actions/workflows/ci.yml/badge.svg)](https://github.com/coolbress/VertexLab/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/coolbress/VertexLab/blob/main/LICENSE)

Fetch financial data from multiple providers and build your own local database — no cloud required, no infrastructure to manage.

Built-in providers today: yfinance and Sharadar.

## Why vertex-forager

Getting financial data into a usable local database is harder than it should be. Rate limits kick in, writes fail halfway through, and large datasets take too long to re-fetch when something goes wrong.

vertex-forager handles the operational complexity so you can focus on the data:

- **Async bulk collection** — fetch large datasets concurrently within API rate limits so workflows that would be slow sequentially can finish much faster
- **Multiple providers, one interface** — switch between yfinance and Sharadar without rewriting your collection flow
- **Automatic rate limiting** — built-in throttling helps you stay within API limits without manual sleep logic
- **Safe local persistence** — schema-aware normalization into DuckDB with idempotent upserts means reruns do not create duplicates
- **Resilient writes** — failed packets can be recovered later instead of forcing a full re-fetch after a transient error
- **Data quality checks** — built-in rules can catch duplicates, future dates, and negative prices before they reach your database
- **No cloud dependency** — everything runs locally on your own machine

## Installation

```bash
pip install "vertex-forager[yfinance] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

vertex-forager is currently distributed from GitHub rather than PyPI. The example below uses the yfinance provider, so the install snippet includes the `yfinance` extra. See [Installation](installation.md) for other install variants, extras, and repository-based workflows.

## Quick Example

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
result = client.get_price_data(tickers=["AAPL", "MSFT"])
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
- [Changelog](https://github.com/coolbress/VertexLab/releases?q=package%3Avertex-forager)
- [Contributing](https://github.com/coolbress/VertexLab/blob/main/CONTRIBUTING.md)
