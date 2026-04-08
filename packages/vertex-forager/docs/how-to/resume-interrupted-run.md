# Resume Interrupted Run

Use this guide when a persisted run stopped partway through and you want to continue from the latest checkpoint instead of starting over.

## Know when resume is possible

Resume only works when all of the following are true:

- the original run used persisted output, not in-memory mode
- a checkpoint exists for the table
- you can construct the provider client again
- you provide a persisted `output` destination for the resumed run

## Resume when a checkpoint exists

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

What this does:

- looks up the latest checkpoint by `table`
- rebuilds the pending job queue from the stored checkpoint payload
- re-enters the normal pipeline with the provided client and output

## Handle the case where no checkpoint exists

If there is no checkpoint for the table, `CheckpointNotFoundError` is raised.

```python
from vertex_forager import CheckpointNotFoundError

try:
    state.checkpoints.resume(
        table="sharadar_price",
        client=client,
        output="duckdb:///forager.duckdb",
    )
except CheckpointNotFoundError:
    print("No resumable checkpoint exists for sharadar_price")
```

When this happens, start a fresh collection instead.

## Avoid in-memory output

Resume requires persisted output. If `output` resolves to an in-memory writer path, resume fails because checkpoint replay has nowhere durable to write.

Use this:

```python
output="duckdb:///forager.duckdb"
```

Do not use resume as if it were an in-memory fetch helper.

## Resume from the CLI

```bash
vertex-forager checkpoints resume --table sharadar_price --output duckdb:///forager.duckdb
```

This command still needs provider credentials in the environment because it constructs the provider client internally.

## Clean up stale checkpoints

If you know a checkpoint is no longer useful:

```python
deleted = state.checkpoints.clear(table="sharadar_price")
print(deleted)
```

## Next steps

- Inspect the rest of local state: [Manage local state](manage-local-state.md)
- Understand what a checkpoint actually stores: [How checkpoints work](../explanation/how-checkpoints-work.md)
