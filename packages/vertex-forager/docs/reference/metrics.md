# Metrics Reference

Metrics are always collected and merged into `RunResult.metrics_*`.

In practice, API consumers most often inspect `RunResult.metrics_summary`. The counter and histogram containers are the raw inputs used to produce that summary.

## Counters

- `rows_written_total`
- `writer_flushes`
- `errors_total`
- `dlq_spooled_files_total`
- `dlq_rescued_total`
- `dlq_remaining_total`
- `dlq_spool_failed_total`
- `dlq_spooled_files.{table}` — per-table DLQ spool file count
- `dlq_rescued.{table}` — per-table packet rescue count
- `dlq_remaining.{table}` — per-table packets still pending replay
- `schema_missing_cols_filled` — cells filled as `null` for missing schema columns in non‑strict mode
- `schema_extra_cols_preserved_count` — number of extra input columns preserved beyond schema
- `inmem_dedup_dropped_rows` — rows removed by in‑memory unique_key dedup/upsert (keep="last")

## Histograms

- Global:
  - `fetch_duration_s`
  - `parse_duration_s`
  - `http_duration_s`
  - `writer_flush_duration_s`
  - `writer_rows`
- Per table:
  - `writer_flush_duration_s.{table}`
  - `writer_rows.{table}`

## Snapshots

These are point-in-time queue measurements captured at specific lifecycle boundaries, not continuously sampled time-series metrics.

- `req_q_len_after_producer`
- `req_q_len_after_req_join`
- `pkt_q_len_after_producer`
- `pkt_q_len_after_pkt_join`

## Summaries

- Global p95/p99 durations
- Per‑table p50/p95/p99 for `writer_flush_duration_s.{table}`, `writer_rows.{table}`

Typical flat summary keys include:

- `http_duration_s_p95`
- `writer_flush_duration_s_p99`
- `writer_flush_duration_s.yfinance_price_p95`
- `writer_rows.yfinance_price_p99`
