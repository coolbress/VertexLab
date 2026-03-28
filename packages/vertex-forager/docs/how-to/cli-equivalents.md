# CLI Equivalents

Common operations with both code and CLI.

> Note
> The `vertex-forager collect` CLI currently accepts only `--symbol` / `-s` and `--source`, and it does not write to DuckDB. Use the Python API when you need DuckDB persistence or DLQ configuration.

## Fetch price data (Sharadar) from the CLI

- Code
  - See `create_client(...).get_price_data(...)` in the quickstart and examples when you need persistence.
- CLI
  - `vertex-forager collect -s AAPL -s MSFT --source sharadar`

## Persist to DuckDB with the Python API

- Code
  - `create_client(...).get_price_data(..., connect_db="duckdb://./forager.duckdb")`
- CLI
  - Not available today; `collect` prints results and summaries instead of writing to DuckDB.

## Operate with DLQ disabled

- Code
  - `EngineConfig(dlq_enabled=False, ...)`
- CLI
  - Not available today; DLQ toggles are configured through the Python API and engine config.

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
