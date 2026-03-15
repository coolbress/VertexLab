# Tune Chunked Flush Thresholds

Chunked flush reduces memory peaks by writing in bounded batches. Tune thresholds for your data volume and disk throughput.

## Concepts

- `flush_threshold_rows`: rows buffered per table before a flush is triggered.
- `writer_chunk_rows`: per‑chunk row target during a flush (>= 10_000 when set).

## Guidelines

- Start with:
  - `flush_threshold_rows`: 500_000 (default)
  - `writer_chunk_rows`: 10_000–50_000, depending on row width and disk speed.
- Increase chunk size if:
  - Write throughput is low but memory headroom is ample.
- Decrease chunk size if:
  - Peak memory or GC pressure increases during merges.

## Example

```python
from vertex_forager.core.config import EngineConfig

cfg = EngineConfig(
    requests_per_minute=120,
    flush_threshold_rows=500_000,
    writer_chunk_rows=20_000,  # must be >= 10_000 when specified
    metrics_enabled=True,      # observe writer_rows.* histograms
)
```

Monitor:
- `writer_rows.{table}` histogram distribution
- `writer_flush_duration_s.{table}` percentiles

Aim for per‑chunk durations within ~0.5–2s on your hardware to balance I/O and latency.
