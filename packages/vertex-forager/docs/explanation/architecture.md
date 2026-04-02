# Pipeline Architecture

```mermaid
flowchart TD
  A[Router Jobs] --> B[Throttle (GCRA) + Concurrency (Gradient)]
  B --> C[HTTP / Library Fetch with Retry]
  C --> D[Parse -> FramePackets]
  D --> E[Normalize (Schemas, PK)]
  E --> F{Flush threshold reached?}
  F -- No --> E
  F -- Yes --> G[Merge Frames, PK Checks]
  G --> H{Write Chunk}
  H -- Success --> I[Update RunResult & Metrics]
  H -- Failure --> J[Per-packet Rescue]
  J -- Partial Success --> K[DLQ Spool Failed Packets]
  J -- All Fail --> K
  K --> L[Operator Recovery CLI]
  L --> H
```

## High‑level Flow

1) Fetch jobs scheduled with `FlowController`:
   - GCRA pacing enforces RPM.
   - Gradient concurrency limits in‑flight requests via RTT feedback.
2) Parse & normalize with Polars and provider schemas.
3) Buffer packets per table until `flush_threshold_rows`.
4) Flush and write:
   - Streaming chunked merge/write using internal defaults.
   - PK validation and per‑chunk accounting.
5) Error handling and DLQ:
   - Attempt per‑packet rescue writes.
   - On remaining failures:
     - Spool to DLQ with atomic replace and fsync.

## Design Notes

- Transport decoupling: routers are transport‑agnostic; library calls use schemes.
- Resilience first: DLQ, structured logs, and per‑table counts maintain operator visibility.
- Memory control: chunked flush avoids monolithic merges on large batches.
- Observability: counters/histograms/optional spans provide diagnostics without hard dependency on external tracing.
