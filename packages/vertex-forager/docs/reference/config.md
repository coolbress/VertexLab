# Configuration Reference

Public runtime configuration is now centered on `create_client(...)` plus grouped config models.

## Required Inputs

- `provider: str` — provider identifier such as `"sharadar"` or `"yfinance"`
- `api_key: str | None` — required for Sharadar, unused for YFinance
- `rate_limit: int` — requests per minute

## Top-Level Client Parameters

- `structured_logs: bool = False`
- `log_verbose: bool = False`
- `schedule: SchedulerConfig = SchedulerConfig()`
- `concurrency: int | None = None`
- `flush_threshold_rows: int`
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
- `rpm_floor_ratio: float` — in `[0, 1]`
- `recovery_factor: float` — in `[0, 1]`
- `healthy_window_s: int`

### HTTPConfig

- `max_connections: int`
- `max_keepalive_connections: int`

### AdvancedConfig

- `tracer: Any | None`
- `otel_enabled: bool | None`

### SchedulerConfig

- `quantum: int = 3`
- `max_pending_per_symbol: int | None = None`
- `backpressure_threshold: int | None = None`

DLQ spooling, periodic cleanup, writer chunking, writer worker count, and memory guard thresholds are internal defaults and are no longer public tuning knobs.

## Example

```python
from vertex_forager import (
    AdaptiveThrottleConfig,
    AdvancedConfig,
    HTTPConfig,
    RetryConfig,
    SchedulerConfig,
    create_client,
)

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=300,
    concurrency=4,
    checkpoint_retention_days=7,
    run_history_retention_days=90,
    schedule=SchedulerConfig(
        quantum=3,
        max_pending_per_symbol=50,
        backpressure_threshold=120,
    ),
    retry=RetryConfig(max_attempts=3),
    throttle=AdaptiveThrottleConfig(enabled=False),
    limits=HTTPConfig(max_connections=200, max_keepalive_connections=100),
    advanced=AdvancedConfig(),
)
```

With `schedule=SchedulerConfig(quantum=3)`, if AAPL has 10 pages, the engine processes up to 3 pagination follow-ups in one DRR round, then rotates to the next symbol with pending pages.

## Pagination Fairness

`SchedulerConfig.quantum` controls the DRR credits issued to each symbol per round.

- Initial jobs still enter the main request queue first.
- Pagination follow-up jobs are grouped into per-symbol queues and scheduled with Deficit Round Robin.
- A larger value favors deeper progress for one symbol before rotation.
- A smaller value rotates more often across symbols with pagination backlog.
- `max_pending_per_symbol` blocks enqueue when one symbol's DRR queue reaches the configured depth.
- `backpressure_threshold` blocks new pagination enqueue once total pending DRR work reaches the configured threshold.

Practical examples:

- `schedule=SchedulerConfig(quantum=3)`
  - AAPL can spend up to 3 pagination credits in one DRR round, then rotation moves to the next symbol with pending pages.
- `schedule=SchedulerConfig(quantum=1)`
  - Pagination backlog behaves like strict symbol-by-symbol round robin.
- `schedule=SchedulerConfig(quantum=3, backpressure_threshold=concurrency * 3 * 10)`
  - Use `concurrency × quantum × 10` as a starting point for total DRR backlog control.

## Shutdown Semantics

The pipeline has two shutdown paths. See `_stop_impl()`, `stop()`, `_try_flush_once()`, and `_persist_packets_with_dlq()` in `pipeline.py` for implementation details.

### Normal completion (`run()` → `_pipeline_orchestration()`)

1. Producer finishes generating jobs and completes.
2. `req_q.join()` waits for the request queue to drain, and the DRR pagination state also drains before shutdown continues.
3. One sentinel per worker is pushed to `req_q`; each fetch worker exits after receiving its sentinel.
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

- Metrics and DLQ handling are always on.
- Writer chunking and memory guard thresholds use internal constants rather than public config.
