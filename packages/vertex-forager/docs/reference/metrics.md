# Metrics Reference

Enable with `EngineConfig.metrics_enabled=True`. When disabled, counters and histograms are not collected or merged into `RunResult.metrics_*`.

## Counters
- `rows_written_total`
- `writer_flushes`
- `errors_total`
- `dlq_spooled_files_total`
- `dlq_rescued_total`
- `dlq_remaining_total`
- `dlq_spool_failed_total`
- `schema_missing_cols_filled` — cells filled as `null` for missing schema columns in non‑strict mode
- `schema_extra_cols_preserved_count` — number of extra input columns preserved beyond schema
- `inmem_dedup_dropped_rows` — rows removed by in‑memory unique_key dedup/upsert (keep="last")

## Histograms
- Global:
  - `fetch_duration_s`
  - `parse_duration_s`
  - `http_duration_s`
  - `writer_flush_duration_s`
- Per table:
  - `writer_flush_duration_s.{table}`
  - `writer_rows.{table}`

## Snapshots
- `req_q_len_after_producer`
- `req_q_len_after_req_join`
- `pkt_q_len_after_producer`
- `pkt_q_len_after_pkt_join`

## Summaries
- Global p95/p99 durations
- Per‑table p50/p95/p99 for `writer_flush_duration_s.{table}`, `writer_rows.{table}`
