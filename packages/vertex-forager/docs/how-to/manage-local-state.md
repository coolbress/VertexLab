# Manage Local State

Use this guide when you want to inspect or operate on the local `state.db` that vertex-forager maintains for persisted runs.

## Create a StateManager

```python
from vertex_forager import StateManager

state = StateManager()
print(state.db_path)
```

`StateManager` is the credential-free entry point for:

- run history
- DLQ index and replay
- checkpoints

## Inspect run history

List recent rows for one table:

```python
from vertex_forager import StateManager

state = StateManager()
records = state.runs.list(table="yfinance_price", limit=10)
for record in records:
    print(record.run_id, record.table_name, record.tables, record.error_count)
```

Clear run history for one table or older records:

```python
deleted = state.runs.clear(table="yfinance_price", before_days=30)
print(deleted)
```

## Inspect DLQ state

List pending DLQ entries:

```python
entries = state.dlq.list(table="sharadar_price", status="pending")
for entry in entries:
    print(entry.table, entry.path, entry.row_count, entry.retry_count)
```

List all statuses:

```python
entries = state.dlq.list(status=None)
```

## Replay failed writes

Replay pending DLQ entries back into DuckDB:

```python
result = state.dlq.replay(
    table="sharadar_price",
    output="duckdb:///forager.duckdb",
)
print(result.replayed, result.failed, result.skipped)
```

Use `dry_run=True` to enumerate without writing:

```python
preview = state.dlq.replay(
    table="sharadar_price",
    output="duckdb:///forager.duckdb",
    dry_run=True,
)
```

## Resume an interrupted run

Checkpoint resume needs a client because vertex-forager must fetch the still-pending jobs again.

```python
from vertex_forager import StateManager, create_client

client = create_client(
    provider="sharadar",
    api_key="YOUR_SHARADAR_API_KEY",
    rate_limit=300,
)
state = StateManager()
result = state.checkpoints.resume(
    table="sharadar_price",
    client=client,
    output="duckdb:///forager.duckdb",
)
print(result.tables)
```

## Clear stale state

Clear DLQ rows and files:

```python
deleted = state.dlq.clear(table="sharadar_price")
```

Clear run history:

```python
deleted = state.runs.clear(table="sharadar_price", before_days=90)
```

Clear checkpoints:

```python
deleted = state.checkpoints.clear(table="sharadar_price")
```

## Use the same workflows from the CLI

```bash
vertex-forager runs list --table sharadar_price --limit 10
vertex-forager dlq list --table sharadar_price --status pending
vertex-forager dlq replay --table sharadar_price
vertex-forager checkpoints resume --table sharadar_price --output duckdb:///forager.duckdb
```

## When credentials are required

- `state.runs.*` does not need provider credentials
- `state.dlq.*` does not need provider credentials
- `state.checkpoints.clear(...)` does not need provider credentials
- `state.checkpoints.resume(...)` does need a provider client because it re-enters the fetch pipeline

## Next steps

- Resume a checkpointed run: [Resume interrupted run](resume-interrupted-run.md)
- Understand what lives in `state.db`: [How checkpoints work](../explanation/how-checkpoints-work.md)
- See exact method signatures: [StateManager reference](../reference/statemanager.md)
