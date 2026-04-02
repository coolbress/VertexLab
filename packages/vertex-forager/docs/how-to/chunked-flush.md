# Tune Chunked Flush Thresholds

Chunked flush reduces memory peaks by writing in bounded batches. Tune thresholds for your data volume and disk throughput.

## Concepts

- `flush_threshold_rows`: rows buffered per table before a flush is triggered.
- `WRITER_CHUNK_ROWS`: internal per‑chunk row target during a flush.

## Guidelines

- Start with:
  - `flush_threshold_rows`: 500_000 (default)
  - Keep `flush_threshold_rows` at default and rely on internal chunking unless you have a clear reason to tune flush cadence.
- Increase `flush_threshold_rows` if:
  - Write throughput is low but memory headroom is ample.
- Decrease `flush_threshold_rows` if:
  - Peak memory or GC pressure increases during merges.

Note: chunk size (`WRITER_CHUNK_ROWS`) is an internal constant and not user-configurable.

## Example

```python
from vertex_forager import create_client

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=120,
    flush_threshold_rows=500_000,
)
```

Monitor:

- `writer_rows.{table}` histogram distribution
- `writer_flush_duration_s.{table}` percentiles

Aim for per‑chunk durations within ~0.5–2s on your hardware to balance I/O and latency.
