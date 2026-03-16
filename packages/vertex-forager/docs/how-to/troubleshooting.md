# Troubleshooting

## Rate limits (HTTP 429) or throttling

- Symptoms
  - Frequent 429 responses, long waits between requests.
- Checks
  - Verify `requests_per_minute` and FlowController downshift settings.
  - Inspect structured logs for `http_retry_reason:*` and `record_feedback`.
- Actions
  - Lower `EngineConfig.requests_per_minute` or enable downshift:
    - `downshift_enabled=True`, `error_rate_threshold` tuned conservatively, `rpm_floor` set to safe minimum.
  - Increase retry backoff within safe bounds (`base_backoff_s`, `max_backoff_s`).

## Memory peaks during flush

- Symptoms
  - High RSS or OOM during large-table writes.
- Checks
  - Review `writer_rows.{table}` and `writer_flush_duration_s.{table}` histograms.
- Actions
  - Enable chunked flush and tune:
    - Set `writer_chunk_rows` (>= 10_000).
    - Adjust `flush_threshold_rows` to control batch sizes.
  - Consider splitting workloads by dataset or symbols if necessary.

## DLQ disabled path

- Symptoms
  - No DLQ files on failure; only summaries.
- Checks
  - Confirm `EngineConfig.dlq_enabled=False` intentionally set.
  - Inspect `RunResult.dlq_counts` and `RunResult.errors` for `DLQ=disabled` summary.
- Actions
  - If persistence is required, set `dlq_enabled=True` and ensure app root is writable.
  - Use recovery CLI to reinject DLQ artifacts when spooling is enabled.

## Writer validation failures (PK missing/null)

- Symptoms
  - Errors mention missing/null PK columns; rows not written.
- Checks
  - Confirm provider schema’s `unique_key` and mapper normalization.
- Actions
  - Fix source or normalization to populate PKs.
  - Use DLQ recovery flow to reinject corrected frames.

## Connectivity

- Symptoms
  - Timeouts or connection pool exhaustion.
- Actions
  - Tune HTTP: `timeout_s`, `max_connections`, `max_keepalive_connections`.
  - Reduce concurrency or increase keepalive where appropriate.
