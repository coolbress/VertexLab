# Quickstart

Today, the practical end-to-end path in VertexLab is powered by `vertex-forager`. This quickstart shows the current install → collect → inspect flow while `vertex-qt` and `vertex-workspace` continue to evolve.

> More end-to-end examples will be added here as `vertex-qt` and `vertex-workspace` implementation work is completed.

## 1. Install

```bash
git clone https://github.com/coolbress/VertexLab.git vertex-lab
cd vertex-lab
uv sync --group dev
```

## 2. Collect Data

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
result = client.get_price_data(
    tickers=["AAPL", "MSFT"],
    connect_db="duckdb://./vertexlab.duckdb",
)
print(result)
```

## 3. Inspect The Result

```python
import duckdb

conn = duckdb.connect("vertexlab.duckdb")
print(conn.sql("show tables").fetchall())
```

## 4. Where To Go Next

- Package docs: [vertex-forager](https://coolbress.github.io/VertexLab/vertex-forager/)
- Installation details: [Installation](installation.md)
- Contribution workflow: [Contributing](contributing.md)
- Release history: [Changelog](changelog.md)

## Coming Soon

- `vertex-qt` for quantitative analysis and backtesting
- `vertex-workspace` for the unified local dashboard experience
- A richer root-level example and concrete dashboard preview will be added once those components are implemented
