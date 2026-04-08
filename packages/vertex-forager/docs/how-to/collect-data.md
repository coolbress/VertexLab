# Collect Data

Use this guide when you want to run a collection workflow and understand what changes when you stay in memory versus writing to DuckDB.

## Choose in-memory or persisted mode

### In-memory mode

Omit `connect_db` when you want a quick result in memory.

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
result = client.get_price_data(tickers=["AAPL", "MSFT"])
if result.data is not None:
    print(result.data.head())
else:
    print("No in-memory DataFrame was returned.")
```

Use this when:

- you want a DataFrame right away
- you are exploring or debugging
- you do not need checkpoints, run history, or DLQ replay

In in-memory mode, `client.get_price_data(...)` still returns `RunResult`. The in-memory DataFrame is available as `result.data`.

### Persisted mode

Pass `connect_db` when you want DuckDB persistence and operational state.

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
result = client.get_price_data(
    tickers=["AAPL", "MSFT"],
    connect_db="duckdb:///forager.duckdb",
)
print(result.tables)
```

Use this when:

- you want durable local tables
- you want checkpoints for interrupted runs
- you want run history and DLQ management in `state.db`

## Run a one-shot collection

```python
from vertex_forager import create_client

client = create_client(provider="sharadar", api_key="YOUR_SHARADAR_API_KEY", rate_limit=300)
result = client.get_fundamental_data(
    tickers=["AAPL"],
    dimension="MRT",
    connect_db="duckdb:///forager.duckdb",
)
```

This is the right path when you want a single fetch and write cycle now.

## Run repeated or scheduled collection

Use `schedule=SchedulerConfig(...)` when you want to influence queue fairness and pending-work behavior for larger runs.

```python
from vertex_forager import SchedulerConfig, create_client

client = create_client(
    provider="sharadar",
    api_key="YOUR_SHARADAR_API_KEY",
    rate_limit=300,
    schedule=SchedulerConfig(
        quantum=3,
        max_pending_per_symbol=50,
    ),
)
```

This is still one run at a time, but the scheduler changes how work is interleaved inside the pipeline.

## Use the async collection pattern

Public async methods follow the same naming as the sync methods with an `_async` suffix.

```python
import asyncio

from vertex_forager import create_client

async def main() -> None:
    client = create_client(provider="yfinance")
    result = await client.get_price_data_async(
        tickers=["AAPL", "MSFT"],
        connect_db="duckdb:///forager.duckdb",
    )
    print(result.tables)


asyncio.run(main())
```

## Read RunResult after a persisted run

Persisted runs return `RunResult`.

### Table counts

```python
print(result.tables)
```

Use this to see which tables were written and how many rows each received.

### Errors

```python
print(result.errors)
```

Use this to inspect partial failures without guessing from logs alone.

### Quality violations

```python
print(result.quality_violations)
```

Use this to see which tables triggered rule violations and how many rows were involved.

### DLQ counts

```python
print(result.dlq_counts)
```

Use this to tell the difference between:

- packets that were rescued immediately
- packets still pending replay

## Run the same workflows from the CLI

```bash
vertex-forager collect sharadar price --symbol AAPL --connect-db duckdb:///forager.duckdb
vertex-forager collect sharadar fundamentals --symbol AAPL --dimension MRT --connect-db duckdb:///forager.duckdb
vertex-forager collect yfinance financials --symbol AAPL --kind income_stmt --period annual --connect-db duckdb:///forager.duckdb
```

## Decide what to do after a run

- If the run completed and wrote rows, inspect `RunResult.tables`
- If the run has partial failures, inspect `RunResult.errors` and `RunResult.dlq_counts`
- If the run was interrupted, use [Resume interrupted run](resume-interrupted-run.md)
- If you need to inspect local state, use [Manage local state](manage-local-state.md)

## Next steps

- Interpret the result object in detail: [Interpret run results](interpret-run-results.md)
- Inspect local state after a persisted run: [Manage local state](manage-local-state.md)
- See the exact CLI shape: [CLI reference](../reference/cli.md)
