# Configuration Reference

Public runtime configuration is now centered on `create_client(...)` plus grouped config models.

## Required Inputs

- `provider: str` — provider identifier such as `"sharadar"` or `"yfinance"`
- `api_key: str | None` — required for Sharadar, unused for YFinance
- `rate_limit: int` — requests per minute

## Top-Level Client Parameters

- `metrics_enabled: bool = False`
- `structured_logs: bool = False`
- `log_verbose: bool = False`
- `dlq_enabled: bool = True`
- `pagination_max_burst: int | None = None`
- `concurrency: int | None = None`
- `flush_threshold_rows: int`
- `writer_chunk_rows: int | None = None`
- `writer_concurrency: int = 1`
- `checkpoint_retention_days: int = 7`
- `run_history_retention_days: int = 90`
- `http_timeout_s: float`

## Grouped Public Config

### RetryConfig

- `max_attempts: int`
- `base_backoff_s: float`
- `max_backoff_s: float` — must be `>= base_backoff_s`
- `backoff_mode: Literal["full_jitter", "equal"]`
- `retry_status_codes: tuple[int, ...]`

### AdaptiveThrottleConfig

- `enabled: bool`
- `window_s: int`
- `error_rate_threshold: float` — in `[0, 1]`
- `rpm_floor: int`
- `recovery_step: int`
- `healthy_window_s: int`

### HTTPConfig

- `max_connections: int`
- `max_keepalive_connections: int`

### AdvancedConfig

- `tracer: Any | None`
- `otel_enabled: bool | None`
- `mem_threshold_ratio: float`
- `mem_threshold_abs_mb: int | None`

DLQ temporary-file cleanup remains an internal housekeeping behavior. `dlq_tmp_cleanup_on_error`, `dlq_tmp_periodic_cleanup`, and `dlq_tmp_retention_s` are not intended as user-facing tuning knobs.

## Example

```python
from vertex_forager import AdaptiveThrottleConfig, AdvancedConfig, HTTPConfig, RetryConfig, create_client

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=300,
    metrics_enabled=True,
    concurrency=4,
    checkpoint_retention_days=7,
    run_history_retention_days=90,
    pagination_max_burst=3,
    retry=RetryConfig(max_attempts=3),
    throttle=AdaptiveThrottleConfig(enabled=False),
    limits=HTTPConfig(max_connections=200, max_keepalive_connections=100),
    advanced=AdvancedConfig(),
)
```

With `pagination_max_burst=3`, if AAPL has 10 pages, the engine processes pages 1–3, then yields to MSFT/GOOG, then resumes AAPL pages 4–6, and so on.

## Shutdown Semantics

The pipeline has two shutdown paths. See `_stop_impl()`, `stop()`, `_try_flush_once()`, and `_persist_packets_with_dlq()` in `pipeline.py` for implementation details.

### Normal completion (`run()` → `_pipeline_orchestration()`)

1. Producer finishes generating jobs and completes.
2. `req_q.join()` waits for all jobs to be processed by fetch workers.
3. One sentinel per worker is pushed to `req_q`; each fetch worker exits after receiving its sentinel. Deferred demoted jobs are logged and dropped.
4. `pkt_q.join()` waits for all packets to be consumed by writers.
5. Writer sentinels are pushed to `pkt_q`; writers exit normally.
6. `_try_flush_once(consume=True)` flushes any buffered data in the writer.
7. `run()` calls `stop()` in its `finally` block for cleanup (idempotent).

### Forced stop (`stop()` → `_stop_impl()`)

1. Non-writer tasks (producer, fetch workers) are cancelled via `task.cancel()`.
2. Best-effort sentinel injection to `req_q` (non-blocking, skips on full queue).
3. Sentinel injection to `pkt_q` for live writers (async with 10 s timeout).
4. If the sentinel timeout fires: writers are cancelled, `pkt_q` is drained, and remaining packets are persisted via `_persist_packets_with_dlq()`.
5. Remaining writer tasks are awaited (`return_exceptions=True`).
6. Safety-net drain: any packets still in `pkt_q` (e.g. when all writers exited before sentinels) are drained and persisted via DLQ.
7. `_try_flush_once(consume=False)` ensures the writer is flushed exactly once.
8. Parse executor is shut down via `asyncio.to_thread(shutdown)`.

### Lifecycle guard

`run()` uses a `_running` flag to prevent concurrent pipeline executions on the same instance. A second `run()` call while the pipeline is active raises `RuntimeError`.

## Notes

- When `dlq_enabled=False`, DLQ files are not written. Summaries include `DLQ=disabled…`, and per‑table counts populate `RunResult.dlq_counts`.
- When `metrics_enabled=False`, counters/histograms are not emitted, but `RunResult.dlq_counts` and summaries still populate.
