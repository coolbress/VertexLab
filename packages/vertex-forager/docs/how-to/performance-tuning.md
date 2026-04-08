# Performance Tuning

## Goals

- Profile realistic workloads and quantify bottlenecks (p95/p99).
- Optimize Polars transforms, writer batching, progress UI, memory validation.
- Tune concurrency and HTTP client parameters to maximize throughput safely.

## Client Parameters

- `concurrency`: Max concurrent fetch jobs/requests.
- `schedule=SchedulerConfig(...)`: Grouped scheduler controls for DRR fairness and backlog pressure.
- `limits=HTTPConfig(...)`: HTTP connection pool settings (max connections, keepalive, timeout).
- `storage=StorageConfig(...)`: Grouped data-lifecycle and write-path tuning settings.

Internal chunk size, writer worker count, and memory guard thresholds are not public tuning knobs. Tune the public settings above first.

## Convenience Environment Variables

- `VF_PROFILE_OUTPUT_DIR`: Output directory for verification artifacts.
- `SHARADAR_API_KEY`: Optional credential for Sharadar verification runs.

## Profiling Scripts

- Price: `packages/vertex-forager/tests/verification/verify_pipeline_perf.py`
- Financials: `packages/vertex-forager/tests/verification/verify_pipeline_perf_financials.py`
- Sweep: `packages/vertex-forager/tests/verification/verify_pipeline_sweep.py`

Usage:

```bash
uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf.py
```

Outputs JSON summaries (p95/p99 and rows) under the configured output directory.

In practice you are reviewing `RunResult.metrics_summary` style aggregates, not raw time-series telemetry.

Example explicit SDK configuration:

```python
from vertex_forager import HTTPConfig, SchedulerConfig, StorageConfig, create_client

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=500,
    concurrency=12,
    schedule=SchedulerConfig(
        quantum=3,
        backpressure_threshold=12 * 3 * 10,
    ),
    limits=HTTPConfig(
        max_connections=200,
        max_keepalive_connections=100,
        timeout_s=30.0,
    ),
    storage=StorageConfig(
        flush_threshold_rows=500_000,
    ),
)
```

## Tuning Strategy

- Start `concurrency` in [8, 12, 16, 20, 24]; calibrate by provider latency.
- Keep `schedule.quantum=3` as the default baseline, then lower it for more symbol interleaving or raise it when deep pagination dominates and fairness is less important.
- When you need backlog protection, start `schedule.backpressure_threshold` near `concurrency × quantum × 10`.
- Add `schedule.max_pending_per_symbol` only when one symbol can monopolize memory with exceptionally deep history.
- Increase `storage.flush_threshold_rows` to reduce flush frequency on large tables, but remember that each flush is still internally chunked.
- Tune `limits.max_keepalive_connections` and `limits.max_connections` together with `concurrency` so the HTTP pool is not the real bottleneck.
- Split processes per dataset if optimal parameters differ significantly.

## Storage-specific notes

- The writer enables DuckDB WAL auto-checkpointing internally.
- `flush_threshold_rows` changes when a flush begins, not the internal chunk size of the write path.
- If you need to reclaim space after heavy persisted runs, use the DuckDB maintenance path rather than expecting tuning alone to shrink on-disk state.
