# Interpret Run Results

Use this guide when a collection run finished and you need to decide whether to trust it, replay something, or start over.

## Know when you get RunResult

You get `RunResult` for persisted workflows and checkpoint resume flows.

```python
result = client.get_price_data(
    tickers=["AAPL", "MSFT"],
    connect_db="duckdb:///forager.duckdb",
)
```

## Read the important fields first

### `tables`

```python
print(result.tables)
```

This tells you which tables were written and how many rows each received.

### `errors`

```python
print(result.errors)
```

This tells you whether the run had partial failures even if some tables still wrote successfully.

### `quality_violations`

```python
print(result.quality_violations)
```

This shows rule violations by table.

### `dlq_counts`

```python
print(result.dlq_counts)
```

This is where you distinguish:

- rows rescued during the same run
- rows still pending replay

### `coverage_pct`

Use this as a coarse summary of how much requested work the run covered.

## Separate three different failure modes

### Errors

Use `result.errors` when the fetch, parse, normalize, or write path raised run-level failures.

### Quality violations

Use `result.quality_violations` when the data failed a rule but the handling mode was still able to continue or report.

### DLQ entries

Use `result.dlq_counts` when persistence failed for some packets and vertex-forager spooled them for later replay.

## Decide what action to take

### Re-fetch

Prefer re-fetching when:

- the source data itself was wrong
- normalization logic changed
- the problem happened before a valid packet was persisted to DLQ

### Replay DLQ

Prefer DLQ replay when:

- the data payload itself is already correct
- the failure happened during persistence
- you want to re-write stored IPC payloads into DuckDB

### Resume a checkpoint

Prefer checkpoint resume when:

- the run stopped before completion
- you want to continue pending work rather than rewrite the whole dataset

## Example review flow

```python
print(result.tables)
print(result.errors)
print(result.quality_violations)
print(result.dlq_counts)
```

Ask these questions in order:

- Did the intended table receive rows?
- Are there run errors?
- Are there quality violations?
- Are there pending DLQ packets?

## Next steps

- Operate on persisted state: [Manage local state](manage-local-state.md)
- Understand DLQ semantics: [How DLQ works](../explanation/how-dlq-works.md)
- See the exact result models: [API reference](../reference/api.md)
