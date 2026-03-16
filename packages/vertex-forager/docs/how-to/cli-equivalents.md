# CLI Equivalents

Common operations with both code and CLI.

## Fetch price data (YFinance) and persist to DuckDB

- Code
  - See `examples/advanced_duckdb_metrics.py`
- CLI
  - `vertex-forager collect --source yfinance --symbol "AAPL,MSFT" --db duckdb://./forager.duckdb`

## Operate with DLQ disabled

- Code
  - `EngineConfig(dlq_enabled=False, ...)`
- CLI
  - `vertex-forager collect --source yfinance --symbol "AAPL,MSFT" --db duckdb://./forager.duckdb --dlq-enabled=false`

## Tune chunked flush

- Code
  - `EngineConfig(writer_chunk_rows=20000, flush_threshold_rows=500000, ...)`
- CLI
  - `vertex-forager tune profile --source yfinance --symbol "AAPL,MSFT"`
  - `vertex-forager tune sweep --source yfinance --symbol "AAPL,MSFT"`
  - `vertex-forager tune export-best --output ./best_config.yaml`

## Other useful commands

- Status
  - `vertex-forager status`
- Constants
  - `vertex-forager constants`
- Queues
  - `vertex-forager clear`
  - `vertex-forager recover`
