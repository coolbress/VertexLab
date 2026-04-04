# Quickstart

Follow this tutorial from a minimal in-memory example to a local DuckDB-backed bulk collection workflow.

## Prerequisites

- Python 3.10+
- Install the package from GitHub:

```bash
pip install "vertex-forager[yfinance] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

- Optional notebook extras:

```bash
pip install "vertex-forager[notebook] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

## 1. Create a client

Start with the minimum client setup:

- `provider` selects the built-in data source
- `rate_limit` is required for providers like Sharadar; YFinance uses an internal default through `create_client(...)`

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
```

For Sharadar, pass an API key when creating the client:

```python
from vertex_forager import create_client

client = create_client(
    provider="sharadar",
    api_key="YOUR_SHARADAR_API_KEY",
    rate_limit=500,
)
```

## 2. Fetch data into memory

If you do not pass `connect_db`, the result stays in memory and you get a Polars DataFrame back immediately.

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
prices = client.get_price_data(tickers=["AAPL", "MSFT"])
print(prices.head())
```

This is the right stopping point if you only need a DataFrame for analysis or ad hoc exploration.

## 3. Persist to DuckDB

Add `connect_db` when you want to build or update a local DuckDB database. In persistence mode the method returns a run summary instead of the in-memory DataFrame.

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
run = client.get_price_data(
    tickers=["AAPL", "MSFT"],
    connect_db="duckdb://./forager.duckdb",
)
print(run.tables)
print(run.errors)
```

You can inspect the stored tables directly with DuckDB:

```python
import duckdb

con = duckdb.connect("./forager.duckdb")
print(con.execute("show tables").fetchall())
print(con.execute("select * from yfinance_price limit 5").fetchdf())
con.close()
```

## Next Steps

- Configure concurrency, retries, and grouped runtime settings: see [Configuration](../reference/config.md)
- Tune local collection behavior: see [Performance tuning](../how-to/performance-tuning.md)
- Tune write batch sizes: see [Chunked flush tuning](../how-to/chunked-flush.md)
- Resume runs and operate on local state: see [Resume and recovery](../how-to/resume-and-recovery.md)
