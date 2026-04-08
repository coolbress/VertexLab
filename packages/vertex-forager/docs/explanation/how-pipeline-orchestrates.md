# How Pipeline Orchestrates

vertex-forager uses a staged producer-consumer pipeline instead of a single loop that fetches, parses, and writes one item at a time.

## Queue topology

The flow is:

```text
req_q -> fetch workers -> pkt_q -> writer worker
```

That split matters because request generation, network I/O, parsing, normalization, and writing do not have the same latency profile.

## Producer-consumer stages

### Producer

The producer asks the router for jobs and pushes them into the request queue.

### Fetch workers

Fetch workers pull jobs, run rate limiting and retry, perform transport work, parse the response, normalize frames, and hand the result to the packet queue.

### Writer worker

The writer worker buffers packets by table and flushes them when thresholds are met.

## DRR fairness

The scheduler uses Deficit Round Robin so one large symbol does not starve every other symbol in the queue.

That matters most for:

- long symbol lists
- datasets with uneven page counts
- workflows where one symbol can generate much more follow-up work than another

## Backpressure

Backpressure appears when writing is slower than fetching.

Instead of letting memory grow without bound, the queue and flush thresholds feed pressure back into the fetch side. In practical terms, writer saturation slows upstream progress because packets cannot be drained freely forever.

## Why in-memory and DuckDB diverge

The pipeline is mostly shared through fetch, parse, and normalize stages. The important divergence happens after the packet queue:

- in-memory mode collects normalized frames in memory
- DuckDB mode applies buffering, flush, validation, upsert, and DLQ behavior

That is why persisted runs gain operational features such as checkpoints and replay while in-memory runs stay lightweight.

## Related pages

- [Pipeline architecture](architecture.md)
- [How flow controller works](how-flow-controller-works.md)
- [How the writer works](how-the-writer-works.md)
