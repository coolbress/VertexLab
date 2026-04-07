# Troubleshooting

## Rate limits (HTTP 429) or throttling

- Symptoms
  - Frequent 429 responses, long waits between requests.
- Checks
  - Verify `requests_per_minute` and FlowController adaptive throttle settings.
  - Inspect structured logs for `http_retry_reason:*` and `record_feedback`.
- Actions
  - Lower `rate_limit` or enable adaptive throttle:
    - `throttle=AdaptiveThrottleConfig(enabled=True, error_rate_threshold=..., rpm_floor_ratio=...)`
  - Increase retry backoff within safe bounds (`base_backoff_s`, `max_backoff_s`).

## Memory peaks during flush

- Symptoms
  - High RSS or OOM during large-table writes.
- Checks
  - Review `writer_rows.{table}` and `writer_flush_duration_s.{table}` histograms.
- Actions
  - Adjust `storage.flush_threshold_rows` to control when buffered rows flush into chunked writes.
  - Consider splitting workloads by dataset or symbols if necessary.

## DLQ persistence

- Symptoms
  - No DLQ files on failure, or spool/write errors appear in summaries.
- Checks
  - Inspect `RunResult.dlq_counts` and `RunResult.errors` for `DLQ=spooled` or `DLQ=spool_failed`.
- Actions
  - Ensure the app root is writable.
  - Use `vertex-forager dlq replay --table <table>` to reinject DLQ artifacts.

## Writer validation failures (PK missing/null)

- Symptoms
  - Errors mention missing/null PK columns; rows not written.
- Checks
  - Confirm provider schema’s `unique_key` and mapper normalization.
- Actions
  - Fix source or normalization to populate PKs.
  - Use `vertex-forager dlq replay --table <table>` to reinject corrected frames.

## Connectivity

- Symptoms
  - Timeouts or connection pool exhaustion.
- Actions
  - Tune HTTP: `limits.timeout_s`, `limits.max_connections`, `limits.max_keepalive_connections`.
  - Reduce concurrency or increase keepalive where appropriate.
