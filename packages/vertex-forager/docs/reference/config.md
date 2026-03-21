# EngineConfig Reference

Core fields commonly used in production. See inline type hints in code for the full list.

## Core

- `requests_per_minute: int` — required, > 0
- `concurrency: int | None` — explicit concurrency; if None, auto‑computed

## Retry

- `retry.max_attempts: int` — default conservative
- `retry.base_backoff_s: float`
- `retry.max_backoff_s: float` — must be >= base_backoff_s
- `retry.enable_http_status_retry: bool` — default True
- `retry.retry_status_codes: tuple[int, ...]` — default `(429, 503)`; optionally extend with idempotent codes only

## Flush & Writer

- `flush_threshold_rows: int` — rows per table before flush
- `writer_chunk_rows: int | None` — per‑chunk row target during flush; when set must be `>= 10_000`

## DLQ

- `dlq_enabled: bool` — enable/disable DLQ spooling (default True)
- `dlq_tmp_cleanup_on_error: bool`
- `dlq_tmp_periodic_cleanup: bool`
- `dlq_tmp_retention_s: int`

## Observability

- `metrics_enabled: bool`
- `structured_logs: bool`
- `tracer: Any` — object with `start_span(name, attributes=...)` for optional spans

## Pagination Fairness

- `pagination_max_burst: int | None` — maximum consecutive same-symbol pagination pages before yielding to other symbols. `None` (default) disables the cap, allowing unlimited consecutive pages. When set (e.g., `2`), after processing *burst* pages for one symbol, the engine demotes remaining same-symbol pages and processes a different symbol first, then resumes. Recommended value: `2–5` for multi-symbol fetches.

### Example

```python
config = EngineConfig(
  requests_per_minute=300,
  concurrency=4,
  pagination_max_burst=3,
)
```

With `pagination_max_burst=3`, if AAPL has 10 pages, the engine processes pages 1–3, then yields to MSFT/GOOG, then resumes AAPL pages 4–6, and so on.

## Downshift (Rate)

- `downshift_enabled: bool`
- `downshift_window_s: int`
- `error_rate_threshold: float (0..1)`
- `rpm_floor: int`
- `recovery_step: int`
- `healthy_window_s: int`

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
