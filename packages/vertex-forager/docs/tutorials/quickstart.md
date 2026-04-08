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

## 4. Run the same workflow from the CLI

The CLI mirrors the stable dataset-specific collection surface:

```bash
vertex-forager collect yfinance price \
  --symbol AAPL \
  --symbol MSFT \
  --output duckdb:///forager.duckdb
```

Other common CLI entry points:

```bash
vertex-forager collect sharadar fundamentals --symbol AAPL --dimension MRT --output duckdb:///forager.duckdb
vertex-forager dlq list --status pending
vertex-forager runs list --table yfinance_price --limit 10
vertex-forager checkpoints resume --table sharadar_price --output duckdb:///forager.duckdb
```

## Next Steps

- Configure concurrency, retries, and grouped runtime settings: see [Configuration](../reference/config.md)
- Tune local collection behavior: see [Performance tuning](../how-to/performance-tuning.md)
- Tune write flush thresholds: see [Chunked flush tuning](../how-to/chunked-flush.md)
- See the CLI mapping for more command examples: [CLI equivalents](../how-to/cli-equivalents.md)
- Resume runs and operate on local state: see [Resume and recovery](../how-to/resume-and-recovery.md)
