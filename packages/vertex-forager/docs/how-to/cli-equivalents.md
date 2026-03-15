# CLI Equivalents

Common operations with both code and CLI.

## Fetch price data (YFinance) and persist to DuckDB

- Code
  - See `examples/advanced_yf_duckdb_metrics.py`
- CLI (sketch)
  - `vertex-forager fetch --provider yfinance --dataset price --tickers "AAPL,MSFT" --db duckdb://./forager.duckdb`

## Operate with DLQ disabled

- Code
  - `EngineConfig(dlq_enabled=False, ...)`
- CLI (sketch)
  - `vertex-forager run --dlq-enabled=false ...`

## Tune chunked flush

- Code
  - `EngineConfig(writer_chunk_rows=20000, flush_threshold_rows=500000, ...)`
- CLI (sketch)
  - `vertex-forager tune --chunk-rows 20000 --flush-rows 500000`
