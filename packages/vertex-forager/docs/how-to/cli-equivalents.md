# CLI Equivalents

Common operations with both code and CLI.

## Collect data with dataset-specific commands

- Sharadar price
  - Code: `create_client(provider="sharadar", ...).get_price_data(tickers=["AAPL", "MSFT"], connect_db="duckdb:///data.db")`
  - CLI: `vertex-forager collect sharadar price --symbol AAPL --symbol MSFT --output duckdb:///data.db`
- Sharadar fundamentals
  - Code: `create_client(provider="sharadar", ...).get_fundamental_data(tickers=["AAPL"], dimension="MRT")`
  - CLI: `vertex-forager collect sharadar fundamentals --symbol AAPL --dimension MRT --output duckdb:///data.db`
- Sharadar tickers
  - Code: `create_client(provider="sharadar", ...).get_ticker_info(connect_db="duckdb:///data.db")`
  - CLI: `vertex-forager collect sharadar tickers --output duckdb:///data.db`
- YFinance price
  - Code: `create_client(provider="yfinance").get_price_data(tickers=["AAPL", "MSFT"], start_date="2020-01-01")`
  - CLI: `vertex-forager collect yfinance price --symbol AAPL --symbol MSFT --start-date 2020-01-01`
- YFinance financials
  - Code: `create_client(provider="yfinance").get_financials(tickers=["AAPL"], kind="income_stmt", period="annual")`
  - CLI: `vertex-forager collect yfinance financials --symbol AAPL --kind income_stmt --period annual`
- YFinance info
  - Code: `create_client(provider="yfinance").get_info(tickers=["AAPL"])`
  - CLI: `vertex-forager collect yfinance info --symbol AAPL`
- YFinance dividends
  - Code: `create_client(provider="yfinance").get_actions(tickers=["AAPL"], kind="dividends")`
  - CLI: `vertex-forager collect yfinance dividends --symbol AAPL --output duckdb:///data.db`

All `collect` subcommands support:

- `--output` for DuckDB persistence
- `--quality-check warn|error`
- `--no-progress`

## Tune chunked flush

- Code
  - `from vertex_forager import create_client, StorageConfig; create_client(provider="yfinance", storage=StorageConfig(flush_threshold_rows=500000))`
- CLI
  - `vertex-forager tune profile --source yfinance --symbol "AAPL,MSFT"`
  - `vertex-forager tune sweep --source yfinance --symbol "AAPL,MSFT"`
  - `vertex-forager tune export-best --output ./best_config.yaml`

## Other useful commands

- Status
  - `vertex-forager status`
- Checkpoints
  - `vertex-forager checkpoints resume --table sharadar_price --output duckdb:///data.db`
  - `vertex-forager checkpoints clear --table sharadar_price`
  - `vertex-forager checkpoints clear --all`
- Constants
  - `vertex-forager constants`
- Run history
  - `vertex-forager runs list --table sharadar_price --limit 10`
  - `vertex-forager runs clear --table sharadar_price`
  - `vertex-forager runs clear --before 90d`
- DLQ
  - `vertex-forager dlq list --table sharadar_price --status pending`
  - `vertex-forager dlq replay --table sharadar_price`
  - `vertex-forager dlq replay --table sharadar_price --output duckdb:///other.db`
  - `vertex-forager dlq clear --table sharadar_price`
  - `vertex-forager dlq clear --all`
- Cache cleanup
  - `vertex-forager clear --checkpoints`
  - `vertex-forager clear --runs`
  - `vertex-forager clear --dlq`
  - `vertex-forager clear --all`
