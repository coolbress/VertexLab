# Providers Reference

This page is the catalog of built-in provider datasets, their target DuckDB tables, and their logical keys.

## Sharadar

| Provider | Dataset | Table | Key columns | unique_key |
|---|---|---|---|---|
| sharadar | price | `sharadar_price` | `ticker`, `date`, `open`, `close` | `(provider, ticker, date)` |
| sharadar | tickers | `sharadar_tickers` | `table`, `ticker`, `name`, `category` | `(provider, table, ticker)` |
| sharadar | fundamental | `sharadar_fundamental` | `ticker`, `dimension`, `calendardate`, `reportperiod` | `(provider, ticker, dimension, calendardate, reportperiod)` |
| sharadar | insider | `sharadar_insider` | `ticker`, `filingdate`, `rownum`, `transactionshares` | `(provider, ticker, filingdate, rownum)` |
| sharadar | institutional | `sharadar_institutional` | `ticker`, `calendardate`, `investorname`, `securitytype` | `(provider, ticker, calendardate, investorname, securitytype)` |
| sharadar | actions | `sharadar_actions` | `ticker`, `date`, `action`, `value` | `(provider, ticker, date, action)` |
| sharadar | daily | `sharadar_daily` | `ticker`, `date`, `close`, `volume` | `(provider, ticker, date)` |
| sharadar | sp500 | `sharadar_sp500` | `ticker`, `date`, `action`, `weight` | `(provider, ticker, date, action)` |

## YFinance

| Provider | Dataset | Table | Key columns | unique_key |
|---|---|---|---|---|
| yfinance | info | `yfinance_info` | `ticker`, `currency`, `exchange`, `market_cap` | `(provider, ticker)` |
| yfinance | price | `yfinance_price` | `ticker`, `date`, `open`, `close` | `(provider, ticker, date)` |
| yfinance | dividends | `yfinance_dividends` | `ticker`, `date`, `dividend` | `(provider, ticker, date)` |
| yfinance | splits | `yfinance_splits` | `ticker`, `date`, `split_ratio` | `(provider, ticker, date)` |
| yfinance | actions | `yfinance_actions` | `ticker`, `date`, `action`, `value` | `(provider, ticker, date)` |
| yfinance | calendar | `yfinance_calendar` | `ticker`, `earnings_date`, `event` | `(provider, ticker, earnings_date)` |
| yfinance | recommendations | `yfinance_recommendations` | `ticker`, `period`, `strong_buy`, `buy` | `(provider, ticker, period)` |
| yfinance | news | `yfinance_news` | `ticker`, `id`, `title`, `published_at` | `(provider, ticker, id)` |
| yfinance | financials | `yfinance_financials` | `date`, `provider`, `fetched_at`, `ticker`, `period`, `statement_kind`, `metric`, `value` | `(date, ticker, provider, period, statement_kind, metric)` |
| yfinance | holders | `yfinance_holders` | `ticker`, `holder_type`, `holder`, `date_reported` | `(provider, ticker, holder_type, holder, date_reported)` |
| yfinance | fast_info | `yfinance_fast_info` | `ticker`, `last_price`, `market_cap` | `(provider, ticker)` |
| yfinance | major_holders | `yfinance_major_holders` | `ticker`, `insider_pct`, `institution_pct` | `(provider, ticker)` |
| yfinance | insider_purchases | `yfinance_insider_purchases` | `ticker`, `insider_purchases_last_6m`, `shares` | `(provider, ticker, insider_purchases_last_6m)` |
| yfinance | insider_roster_holders | `yfinance_insider_roster_holders` | `ticker`, `name`, `position`, `latest_transaction_date` | `(provider, ticker, name, position, latest_transaction_date)` |

## Notes

- The stable CLI in issue #324 exposes a task-oriented subset of these datasets directly.
- `collect` subcommands use `--connect-db` for DuckDB persistence, matching the SDK's `connect_db=` parameter.
- The SDK surface still includes the wider built-in provider dataset inventory listed here.
- The exact column types are defined by `TableSchema`; see [Schema reference](schema.md).

## Async usage

Every public sync collection method has a matching `*_async` coroutine on the same client.

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
result = await client.get_price_data_async(
    tickers=["AAPL", "MSFT"],
    connect_db="duckdb:///forager.duckdb",
)
```

Examples:

- `get_price_data()` → `get_price_data_async()`
- `get_fundamental_data()` → `get_fundamental_data_async()`
- `get_info()` → `get_info_async()`
- `get_news()` → `get_news_async()`

## Related pages

- [Schema reference](schema.md)
- [How built-in providers work](../explanation/how-built-in-providers-work.md)
- [CLI reference](cli.md)
