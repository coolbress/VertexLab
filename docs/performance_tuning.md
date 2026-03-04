# Vertex Forager Performance Tuning (Issue #16)

## Goals

- Profile realistic workloads and quantify bottlenecks (p95/p99, duration).
- Optimize Polars transforms, writer batching, progress UI, memory validation.
- Tune concurrency and HTTP client parameters to maximize throughput safely.

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

