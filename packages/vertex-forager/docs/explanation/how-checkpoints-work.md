# How Checkpoints Work

Checkpoints let a persisted run resume pending work without re-fetching everything from scratch.

## What a checkpoint stores

A checkpoint stores the minimum state required to reconstruct interrupted work:

- run ID
- provider and dataset
- completed symbol set
- failed symbol set
- pending jobs queue payload
- table name used for lookup

The key lookup field for the public API is `table_name`.

## When checkpoints are written

Checkpoints are written only for persisted workflows. In-memory runs do not create resumable checkpoint state because there is no durable output target to resume into.

## How resume works

Resume looks up the latest checkpoint by table, validates the provider, and rebuilds the pending job queue from the stored payload.

From there the pipeline re-enters the same orchestration path as a normal persisted run.

In other words, checkpoint resume is not a special mini-run. It reconstructs enough state to continue the pipeline with ordinary fetch, parse, normalize, and write stages.

## Why `table_name` matters

The stable public lookup model is table-based:

- `sharadar_price`
- `sharadar_fundamental`
- `yfinance_price`

That matches how operators think about runs, DLQ, and run history in the rest of the public surface.

## What happens if output is in-memory

Resume requires persisted output. If the caller tries to resume into an in-memory target, the operation fails because checkpointed work must continue into a durable writer path.

## Relationship to run history

Checkpoint state answers “what is still pending?”

Run history answers “what happened in the finished run?”

They solve different operational questions and are stored separately even though both live in `state.db`.

## Related pages

- [Manage local state](../how-to/manage-local-state.md)
- [Resume interrupted run](../how-to/resume-interrupted-run.md)
- [StateManager reference](../reference/statemanager.md)
