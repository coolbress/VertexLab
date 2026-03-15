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

## Downshift (Rate)
- `downshift_enabled: bool`
- `downshift_window_s: int`
- `error_rate_threshold: float (0..1)`
- `rpm_floor: int`
- `recovery_step: int`
- `healthy_window_s: int`

## Notes
- When `dlq_enabled=False`, DLQ files are not written. Summaries include `DLQ=disabled…`, and per‑table counts populate `RunResult.dlq_counts`.
- When `metrics_enabled=False`, counters/histograms are not emitted, but `RunResult.dlq_counts` and summaries still populate.
