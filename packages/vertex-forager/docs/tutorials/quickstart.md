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
- `rate_limit` sets the requests-per-minute target you want the client to respect

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
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

client = create_client(provider="yfinance", rate_limit=60)
prices = client.get_price_data(tickers=["AAPL", "MSFT"])
print(prices.head())
```

This is the right stopping point if you only need a DataFrame for analysis or ad hoc exploration.

## 3. Persist to DuckDB

Add `connect_db` when you want to build or update a local DuckDB database. In persistence mode the method returns a run summary instead of the in-memory DataFrame.

```python
from vertex_forager import create_client

client = create_client(provider="yfinance", rate_limit=60)
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

## 4. Bulk collection

Once the simple flow works, scale it up with environment variables and a larger ticker set. This is a useful pattern for your own local refresh scripts and for the repository examples.

```python
import os

from vertex_forager import create_client

provider = os.getenv("VF_PROVIDER", "yfinance").strip()
tickers = [t.strip() for t in os.getenv("VF_TICKERS", "AAPL,MSFT,GOOGL,AMZN").split(",")]
db_path = os.getenv("VF_DUCKDB_PATH", "./forager.duckdb")

kwargs = {}
if provider == "sharadar":
    kwargs["api_key"] = os.environ["SHARADAR_API_KEY"]
    kwargs["rate_limit"] = 500
else:
    kwargs["rate_limit"] = 60

client = create_client(provider=provider, **kwargs)
run = client.get_price_data(
    tickers=tickers,
    connect_db=f"duckdb://{db_path}",
    show_progress=False,
)
print(run)
```

The same pattern works well with the repository examples:

```bash
git clone https://github.com/coolbress/VertexLab.git
cd VertexLab
VF_PROVIDER=yfinance VF_TICKERS=AAPL,MSFT,GOOGL,AMZN \
VF_DUCKDB_PATH=./forager.duckdb \
uv run python packages/vertex-forager/examples/advanced_duckdb_metrics.py
```

Sharadar requires `SHARADAR_API_KEY` in the environment:

```bash
export SHARADAR_API_KEY=YOUR_KEY
VF_PROVIDER=sharadar VF_TICKERS=AAPL,MSFT \
VF_DUCKDB_PATH=./forager.duckdb \
uv run python packages/vertex-forager/examples/advanced_duckdb_metrics.py
```

## Next Steps

- Configure concurrency, retries, and grouped runtime settings: see [Configuration](../reference/config.md)
- Tune local collection behavior: see [Performance tuning](../how-to/performance-tuning.md)
- Tune write batch sizes: see [Chunked flush tuning](../how-to/chunked-flush.md)
