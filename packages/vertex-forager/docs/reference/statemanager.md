# StateManager Reference

`StateManager` is the public entry point for local persisted runtime state.

## Construction

```python
from vertex_forager import StateManager

state = StateManager()
print(state.db_path)
```

It exposes three namespaces:

- `state.dlq`
- `state.runs`
- `state.checkpoints`

## `dlq`

### `state.dlq.list(table: str | None = None, status: str | None = "pending") -> list[DLQEntry]`

List DLQ index entries.

- `table`
  - optional table filter
- `status`
  - `"pending"` by default
  - `None` returns all statuses

### `state.dlq.replay(table: str, output: str, dry_run: bool = False) -> ReplayResult`

Replay stored IPC payloads for one table.

- `table`
  - required table name
- `output`
  - persisted destination such as `duckdb:///forager.duckdb`
- `dry_run`
  - enumerate and validate without writing

### `state.dlq.clear(table: str | None = None) -> int`

Delete DLQ index rows and associated spool files.

## `runs`

### `state.runs.list(table: str | None = None, limit: int = 20) -> list[RunRecord]`

List persisted run-history rows.

### `state.runs.clear(table: str | None = None, before_days: int | None = None) -> int`

Delete run-history rows.

- `table`
  - optional table filter
- `before_days`
  - optional age cutoff in whole days

## `checkpoints`

### `state.checkpoints.resume(table: str, client, output: str) -> RunResult`

Resume the latest checkpoint for a table.

- `table`
  - required table name
- `client`
  - provider client used to refetch pending jobs
- `output`
  - required persisted destination

Raises:

- `CheckpointNotFoundError`
- provider or persistence errors surfaced through the normal pipeline path

### `state.checkpoints.clear(table: str | None = None) -> int`

Delete checkpoint rows for one table or all tables.

## Result models

### `DLQEntry`

Fields:

- `provider`
- `table`
- `row_count`
- `retry_count`
- `status`
- `created_at`
- `path`

### `ReplayResult`

Fields:

- `replayed`
- `failed`
- `skipped`
- `errors`

### `RunRecord`

Fields:

- `run_id`
- `provider`
- `dataset`
- `table_name`
- `started_at`
- `finished_at`
- `duration_s`
- `tables`
- `error_count`
- `errors`
- `quality_violations`
- `coverage_pct`
- `created_at`

## Thread safety

`StateManager` is safe for normal concurrent access patterns where separate scripts, processes, or threads create their own `StateManager()` instances and operate on the same `state.db`.

That is possible because state operations use short-lived SQLite connections per call, and SQLite serializes writes at the database level.

Recommended usage:

- create a separate `StateManager()` per thread or process
- do not treat one shared `StateManager` instance as a synchronization primitive
- expect concurrent writers to serialize through SQLite rather than run truly in parallel

## Related pages

- [Manage local state](../how-to/manage-local-state.md)
- [API reference](api.md)
