# Performance Tuning

## Goals

- Profile realistic workloads and quantify bottlenecks (p95/p99).
- Optimize Polars transforms, writer batching, progress UI, memory validation.
- Tune concurrency and HTTP client parameters to maximize throughput safely.

## Environment Variables

- `VF_CONCURRENCY`: Max concurrent fetch jobs/requests.
- `VF_FLUSH_THRESHOLD_ROWS`: Buffer rows per table before flush.
- `VF_METRICS_ENABLED`: Enable metrics collection.
- `VF_HTTP_MAX_KEEPALIVE`: HTTP client max keepalive connections.
- `VF_HTTP_MAX_CONNECTIONS`: HTTP client max total connections.
- `VF_HTTP_TIMEOUT_S`: HTTP request timeout in seconds.
- `VF_MEM_THRESHOLD_RATIO`: Available memory ratio threshold.
- `VF_MEM_THRESHOLD_ABS_MB`: Absolute memory threshold in MB.

## Profiling Scripts

- Price: `packages/vertex-forager/tests/verification/verify_pipeline_perf.py`
- Financials: `packages/vertex-forager/tests/verification/verify_pipeline_perf_financials.py`
- Sweep: `packages/vertex-forager/tests/verification/verify_pipeline_sweep.py`

Usage:

```bash
export VF_METRICS_ENABLED=1
uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf.py
```

Outputs JSON summaries (p95/p99 and rows) under the configured output directory.

## Tuning Strategy

- Start `VF_CONCURRENCY` in [8, 12, 16, 20, 24]; calibrate by provider latency.
- Increase `VF_FLUSH_THRESHOLD_ROWS` to reduce flush frequency on large tables.
- Tune HTTP keepalive/connection counts to match concurrency and provider behavior.
- Split processes per dataset if optimal parameters differ significantly.
