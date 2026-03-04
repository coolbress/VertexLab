# Vertex Forager Performance Tuning (Issue #16)

## Goals

- Profile realistic workloads and quantify bottlenecks (p95/p99, duration).
- Optimize Polars transforms, writer batching, progress UI, memory validation.
- Tune concurrency and HTTP client parameters to maximize throughput safely.

## Baseline Performance

> Note: Metrics measured on macOS M1/M2 (local dev environment).
> Commands:
> - Price: `VF_METRICS_ENABLED=1 /usr/bin/time -l uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf.py`
> - Financials: `VF_METRICS_ENABLED=1 /usr/bin/time -l uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf_financials.py`
> Artifacts (example paths with timestamp):
> - Price: `output/forager-profiles/{YYYYMMDD_HHMMSS}/profile_metrics.json`
> - Financials: `output/forager-profiles/{YYYYMMDD_HHMMSS}/profile_financials_metrics.json`
>
> **Pre-Optimization Values**: Estimated based on manual timing (`time` command) due to lack of profiling tools at that stage; not reproducible.

| Metric | Pre-Optimization (Estimated) | Post-Optimization (Measured) | Improvement |
| :--- | :--- | :--- | :--- |
| **Price Data (5 Tickers)** | ~8.5s | **~4.5s** (Wall) / ~2.1s (Fetch p95) <sup>[1]</sup> | ~47% Faster |
| **Financials (10 Tickers)** | ~12.0s | **~4.8s** (Wall) / ~2.0s (Fetch p95) <sup>[2]</sup> | ~60% Faster |
| **Throughput (Price)** | ~5k rows/s | **~9,000 rows/s** <sup>[1]</sup> | ~1.8x Throughput |
| **Throughput (Financials)** | ~150 rows/s | **~430 rows/s** <sup>[2]</sup> | ~2.8x Throughput |
| **Memory Overhead** | High (>500MB) | **~274 MB** (Peak RSS) | Reduced Peak Usage |

*Key Optimizations:*
- **Polars Transforms**: Switched to lazy evaluation and optimized `melt`/`pivot` operations.
- **Writer Batching**: Implemented `VF_FLUSH_THRESHOLD_ROWS` to reduce I/O frequency.
- **Progress UI**: `tqdm` retained and used conditionally (skipped when `show_progress=False`) for minimal overhead.
- **Concurrency**: Tuned `VF_CONCURRENCY` and connection pooling for optimal network utilization.

---
> [1] `$.summary.fetch_duration_s_p95` in `profile_metrics.json`
> [2] `$.summary.fetch_duration_s_p95` in `profile_financials_metrics.json`

## Environment Variables

- VF_CONCURRENCY: Max concurrent fetch jobs/requests (Default: auto-calculated by FlowController).
- VF_FLUSH_THRESHOLD_ROWS: Number of rows to buffer before flushing to writer (Default: 100,000).
- VF_METRICS_ENABLED: Enable metrics collection counters/histograms (Default: 0/False).
- VF_HTTP_MAX_KEEPALIVE: HTTP client max keepalive connections (Default: 100).
- VF_HTTP_MAX_CONNECTIONS: HTTP client max total connections (Default: 200).
- VF_HTTP_TIMEOUT_S: HTTP request timeout in seconds (Default: 30).
- VF_MEM_THRESHOLD_RATIO: Available memory ratio threshold for safety warning (Default: 0.7).
- VF_MEM_THRESHOLD_ABS_MB: Absolute memory threshold in MB for safety warning (Default: 4096).

## Output Location

- Default: output/forager-profiles
- Override: set VF_PROFILE_OUTPUT_DIR to a secure internal path
- Note: output/ is excluded from VCS; generated JSON is not tracked

## CLI Usage

- Profile single run
  - uv run vertex-forager tune profile --kind price
  - uv run vertex-forager tune profile --kind financials
- Sweep combinations
  - uv run vertex-forager tune sweep
  - Include Sharadar SF1: uv run vertex-forager tune sweep --include-sharadar
  - Custom lists:
    - --concurrency-list "10,14,18"
    - --flush-rows-list "120000,180000,240000"
    - --keepalive-list "120,180"
    - --connections-list "240,360"
    - --timeout-list "40,45"
  - Sampling options:
    - --sample-count 20
    - --sample-seed 42
  - Ranking options:
    - --rank-by duration|duration_p95
    - --rank-alpha 0.3
    - --rank-error-penalty 5.0
- Export recommended env
  - uv run vertex-forager tune export-best
  - Write to file:
    - uv run vertex-forager tune export-best --write-file exports.sh

## Profiling Scripts (Alternative)

- Price: packages/vertex-forager/tests/verification/verify_pipeline_perf.py
- Financials: packages/vertex-forager/tests/verification/verify_pipeline_perf_financials.py
- Sweep: packages/vertex-forager/tests/verification/verify_pipeline_sweep.py
- Usage:
  - export VF_METRICS_ENABLED=1
  - uv run python <script>
  - Outputs JSON with counters/histograms/summary (p95/p99 and rows) under VF_PROFILE_OUTPUT_DIR or default output folder

## Tuning Strategy

- Start VF_CONCURRENCY in [8, 12, 16, 20, 24]; calibrate by provider latency.
- Increase VF_FLUSH_THRESHOLD_ROWS to reduce writer flush frequency on large tables.
- Tune HTTP keepalive/connection counts to match concurrency and provider behavior.
- In-memory runs: adjust VF_MEM_THRESHOLD_RATIO or VF_MEM_THRESHOLD_ABS_MB cautiously.
- Operate price and financials with separate processes/env when their optimal parameters differ.

## Sharadar Notes

- BaseClient applies env tuning for SharadarClient identically.
- Sweep/profile includes SF1 MRT when SHARADAR_API_KEY is set.
- Prefer DuckDB persistence for SF1/SF2 large pulls; use in-memory only for small subsets.

## Operational Adoption

- Run sweep with expanded tickers/ranges under VF_PROFILE_OUTPUT_DIR for storage.
- Select best per dataset by duration and p95/p99 stability.
- Apply per-process env exports for price vs financials via tune export-best output.
- Re-run sweep periodically as provider latency patterns evolve.

## Safety

- Do not log secrets; use environment injection only for numeric tuning.
- Monitor errors arrays in outputs; reject parameter sets that introduce failures.

