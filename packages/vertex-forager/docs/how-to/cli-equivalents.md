# CLI Equivalents

Common operations with both code and CLI.

> Note
> The `vertex-forager collect` CLI currently accepts only `--symbol` / `-s` and `--source`, and it does not write to DuckDB. Use the Python API when you need DuckDB persistence.

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

## Tune chunked flush

- Code
  - `create_client(..., flush_threshold_rows=500000)`
- CLI
  - `vertex-forager tune profile --source yfinance --symbol "AAPL,MSFT"`
  - `vertex-forager tune sweep --source yfinance --symbol "AAPL,MSFT"`
  - `vertex-forager tune export-best --output ./best_config.yaml`

## Other useful commands

- Status
  - `vertex-forager status`
- Constants
  - `vertex-forager constants`
- Run history
  - `vertex-forager runs list --limit 10`
  - `vertex-forager runs clear --before 90d`
- DLQ
  - `vertex-forager dlq list`
  - `vertex-forager dlq retry --table sharadar_sep --db ./forager.duckdb`
  - `vertex-forager dlq clear --before 1d`
- Cache cleanup
  - `vertex-forager clear --checkpoints`
  - `vertex-forager clear --runs`
  - `vertex-forager clear --dlq`
  - `vertex-forager clear --all`
  - `vertex-forager recover`
