# Performance Tuning

## Goals

- Profile realistic workloads and quantify bottlenecks (p95/p99).
- Optimize Polars transforms, writer batching, progress UI, memory validation.
- Tune concurrency and HTTP client parameters to maximize throughput safely.

## Client Parameters

- `concurrency`: Max concurrent fetch jobs/requests.
- `flush_threshold_rows`: Buffer rows per table before flush.
- `metrics_enabled`: Enable metrics collection.
- `http_timeout_s`: HTTP request timeout in seconds.
- `limits=HTTPConfig(...)`: HTTP client max keepalive and total connection counts.
- `advanced=AdvancedConfig(...)`: Advanced resource controls such as `mem_threshold_ratio` and `mem_threshold_abs_mb`.

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

Example explicit SDK configuration:

```python
from vertex_forager import AdvancedConfig, HTTPConfig, create_client

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=500,
    metrics_enabled=True,
    concurrency=12,
    flush_threshold_rows=500_000,
    http_timeout_s=30.0,
    limits=HTTPConfig(
        max_connections=200,
        max_keepalive_connections=100,
    ),
    advanced=AdvancedConfig(
        mem_threshold_ratio=0.85,
        mem_threshold_abs_mb=4096,
    ),
)
```

## Tuning Strategy

- Start `concurrency` in [8, 12, 16, 20, 24]; calibrate by provider latency.
- Increase `flush_threshold_rows` to reduce flush frequency on large tables.
- Tune `limits.max_keepalive_connections` and `limits.max_connections` to match concurrency and provider behavior.
- Split processes per dataset if optimal parameters differ significantly.
