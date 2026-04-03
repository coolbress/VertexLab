# Resume and Recovery

`vertex-forager` stores resumable state in a single SQLite database and keeps DLQ payloads as Arrow IPC files.

## Where state lives

Default cache layout:

```text
~/.cache/vertex-forager/
  state.db
  dlq/<table>/
    batch_*.ipc
```

When `VERTEXFORAGER_ROOT` is set, the cache moves under `$VERTEXFORAGER_ROOT/cache/`.

## What each table stores

- `checkpoints`
  - One row per run ID
  - Provider, dataset, completed symbols, failed symbols
  - Used by `resume=True` to skip symbols already completed in a prior run
- `run_history`
  - One row per completed run
  - Run timing, per-table row counts, error count, serialized errors, quality violations, coverage
  - Used by `vertex-forager runs list`
- `dlq_index`
  - One row per spooled DLQ IPC file
  - Spool path, table, provider, row count, created time, retry status
  - Used by `vertex-forager dlq list`, `retry`, and `clear`

## How checkpoints are created

Checkpoints are written automatically at the end of a pipeline run. A completed run persists:

- the run ID
- the provider and dataset
- the completed symbol set
- the failed symbol set

## Resume a run

Use the same provider and dataset with `resume=True`:

```python
from vertex_forager import create_client

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=300,
)

result = client.get_price_data(
    tickers=["AAPL", "MSFT", "NVDA"],
    connect_db="forager.duckdb",
    resume=True,
)
```

When a matching checkpoint exists, completed symbols are skipped automatically.

## Inspect run history

List recent runs:

```bash
uv run vertex-forager runs list --limit 10
```

Delete old run history:

```bash
uv run vertex-forager runs clear --before 90d
```

## Inspect DLQ state

List pending DLQ entries:

```bash
uv run vertex-forager dlq list
```

Retry all pending entries for one table:

```bash
uv run vertex-forager dlq retry --table sharadar_sep --db ./forager.duckdb
```

Delete old DLQ entries and their IPC files:

```bash
uv run vertex-forager dlq clear --before 1d
```

## Configure retention

Retention is applied automatically when a pipeline starts.

```python
from vertex_forager import StorageConfig, create_client

client = create_client(
    provider="yfinance",
    rate_limit=60,
    storage=StorageConfig(
        checkpoint_retention_days=7,
        run_history_retention_days=90,
    ),
)
```

- `storage.checkpoint_retention_days`
  - Default `7`
  - Keeps completed checkpoint rows only as long as they are likely useful for resume flows
- `storage.run_history_retention_days`
  - Default `90`
  - Keeps operational and audit history longer than checkpoints
- DLQ retention
  - Follows `storage.dlq_tmp_retention_s` housekeeping window
  - Configurable via `StorageConfig(dlq_tmp_retention_s=...)`

## Clear state selectively

Clear only checkpoints:

```bash
uv run vertex-forager clear --checkpoints
```

Clear only run history:

```bash
uv run vertex-forager clear --runs
```

Clear only DLQ state:

```bash
uv run vertex-forager clear --dlq
```

Clear everything:

```bash
uv run vertex-forager clear --all
```
