# How DLQ Works

The Dead Letter Queue is vertex-forager’s persistence safety net for packets that could not be written successfully.

## When a packet reaches DLQ

DLQ is not the first thing vertex-forager tries.

The write path is:

1. merge and flush buffered packets
2. if flush fails, attempt per-packet rescue writes
3. only the packets that still fail are spooled to DLQ

This means DLQ contains the residual failures after the normal write path and rescue path have already tried to persist as much as possible.

## What gets written to disk

Failed packets are written as Arrow IPC files under the cache root:

```text
cache/dlq/<table>/batch_<time_ns>.ipc
```

Each file contains the stored payload that could not be written at flush time.

## What `dlq_index` represents

Every spooled IPC file also gets a row in `dlq_index`.

That row records:

- file path
- provider
- table
- row count
- created time
- retry count
- replay status
- original `output_uri`

The stored `output_uri` matters because replay can default to the original destination instead of forcing the operator to supply it every time.

## Status transitions

The important states are:

- `pending`
  - the file still needs replay or manual cleanup
- `recovered`
  - replay succeeded and state was updated accordingly

If replay fails, the entry stays pending and the retry counter increases.

## Atomic write guarantee

DLQ files are written with a temp-file workflow:

1. write the temp file in the same directory
2. flush and fsync it
3. replace the target path with `os.replace`

That pattern avoids partially written final files after a crash in the middle of a spool operation.

## How replay uses DLQ

Replay does not reconstruct data from the source provider. It reads the stored IPC payloads and writes them again through the normal writer path.

That is why replay is appropriate for persistence failures, not for cases where the source data or normalization logic itself was wrong.

## Related pages

- [How checkpoints work](how-checkpoints-work.md)
- [Manage local state](../how-to/manage-local-state.md)
- [StateManager reference](../reference/statemanager.md)
