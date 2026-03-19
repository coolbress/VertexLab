"""YFinance schema definitions for vertex_forager provider.

This module declares public table schemas for datasets fetched via the
Yahoo Finance (yfinance) integration. Schemas define table names,
column types, unique keys, and date-related columns for storage and
analysis with Polars.

Exports:
    TableSchema: Schema configuration dataclass used across providers.
    YFINANCE_INFO_SCHEMA: Schema for ticker info/metadata.
    YFINANCE_PRICE_SCHEMA: Schema for historical OHLCV downloads.
    YFINANCE_FINANCIALS_SCHEMA: Schema for financial statements (income, balance, cashflow).
    YFINANCE_HOLDERS_SCHEMA: Schema for institutional/mutual fund holders.
    YFINANCE_MAJOR_HOLDERS_SCHEMA: Schema for major holders summary.
    YFINANCE_INSIDER_ROSTER_SCHEMA: Schema for insider roster holders.
    YFINANCE_INSIDER_PURCHASES_SCHEMA: Schema for insider purchases.
    YFINANCE_CALENDAR_SCHEMA: Schema for earnings calendar entries.
    YFINANCE_NEWS_SCHEMA: Schema for ticker news articles.

Notes:
    - Polars (pl) is used for column types.
    - All timestamps should be UTC where applicable.
"""

from __future__ import annotations

from typing import Final

import polars as pl
from vertex_forager.constants import DEFAULT_TIME_ZONE
from vertex_forager.providers.yfinance.constants import (
    DATASET_ENDPOINT,
    DATE_FILTER_COL,
)
from vertex_forager.schema.config import TableSchema

# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------

# Info
YFINANCE_INFO_SCHEMA = TableSchema(
    table="yfinance_info",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "shortname": pl.Utf8,
        "longname": pl.Utf8,
        "currency": pl.Utf8,
        "exchange": pl.Utf8,
        "sector": pl.Utf8,
        "industry": pl.Utf8,
        "website": pl.Utf8,
        "address1": pl.Utf8,
        "city": pl.Utf8,
        "state": pl.Utf8,
        "zip": pl.Utf8,
        "country": pl.Utf8,
        "phone": pl.Utf8,
        "language": pl.Utf8,
        "region": pl.Utf8,
        "quotesourcename": pl.Utf8,
        "displayname": pl.Utf8,
        "marketcap": pl.Int64,
        "longbusinesssummary": pl.Utf8,
        "fulltimeemployees": pl.Int64,
        "irwebsite": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker"),
    analysis_date_col=None,
)

YFINANCE_PRICE_SCHEMA = TableSchema(
    table="yfinance_price",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "adj_close": pl.Float64,
        "volume": pl.Float64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
)

YFINANCE_DIVIDENDS_SCHEMA = TableSchema(
    table="yfinance_dividends",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "dividends": pl.Float64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
)

YFINANCE_SPLITS_SCHEMA = TableSchema(
    table="yfinance_splits",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "stock_splits": pl.Float64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
)

YFINANCE_ACTIONS_SCHEMA = TableSchema(
    table="yfinance_actions",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "dividends": pl.Float64,
        "stock_splits": pl.Float64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
)

YFINANCE_CALENDAR_SCHEMA = TableSchema(
    table="yfinance_calendar",
    schema={
        "provider": pl.Utf8,
        "earnings_date": pl.Date,
        "earnings_average": pl.Float64,
        "earnings_low": pl.Float64,
        "earnings_high": pl.Float64,
        "revenue_average": pl.Int64,
        "revenue_low": pl.Int64,
        "revenue_high": pl.Int64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "earnings_date"),
    analysis_date_col="earnings_date",
)

YFINANCE_RECOMMENDATIONS_SCHEMA = TableSchema(
    table="yfinance_recommendations",
    schema={
        "provider": pl.Utf8,
        "period": pl.Utf8,
        "strongbuy": pl.Int64,
        "buy": pl.Int64,
        "hold": pl.Int64,
        "sell": pl.Int64,
        "strongsell": pl.Int64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "period"),
    analysis_date_col=None,
)

# News
YFINANCE_NEWS_SCHEMA = TableSchema(
    table="yfinance_news",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "id": pl.Utf8,
        "title": pl.Utf8,
        "publisher": pl.Utf8,
        "type": pl.Utf8,
        "link": pl.Utf8,
        "published_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "id", "published_at"),
    analysis_date_col="published_at",
)
# For Financials (Balance Sheet, Income Stmt, Cashflow)
# Converted to Long Format (Unpivoted) to handle dynamic account items (metrics).
# Structure: date | ticker | metric | value
YFINANCE_FINANCIALS_SCHEMA = TableSchema(
    table="yfinance_financials",
    schema={
        "date": pl.Date,
        "ticker": pl.Utf8,
        "provider": pl.Utf8,
        "period": pl.Utf8,
        "metric": pl.Utf8,
        "value": pl.Float64,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("date", "ticker", "provider", "period", "metric"),
    analysis_date_col="date",
)

# For Holders
YFINANCE_HOLDERS_SCHEMA = TableSchema(
    table="yfinance_holders",
    schema={
        "provider": pl.Utf8,
        "holder": pl.Utf8,
        "shares": pl.Float64,
        "date_reported": pl.Date,
        "percentage_out": pl.Float64,
        "pctheld": pl.Float64,
        "pctchange": pl.Float64,
        "value": pl.Float64,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "holder", "date_reported"),
    analysis_date_col="date_reported",
)

YFINANCE_FAST_INFO_SCHEMA = TableSchema(
    table="yfinance_fast_info",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker"),
    analysis_date_col=None,
    flexible_schema=True,
)
YFINANCE_MAJOR_HOLDERS_SCHEMA = TableSchema(
    table="yfinance_major_holders",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "insiders_percent_held": pl.Float64,
        "institutions_count": pl.Float64,
        "institutions_float_percent_held": pl.Float64,
        "institutions_percent_held": pl.Float64,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker"),
    analysis_date_col=None,
)

YFINANCE_INSIDER_PURCHASES_SCHEMA = TableSchema(
    table="yfinance_insider_purchases",
    schema={
        "provider": pl.Utf8,
        "holder": pl.Utf8,
        "shares": pl.Float64,
        "trans": pl.Int64,  # Number of transactions? Or shares transacted? yfinance says 'Shares' and 'Trans'
        "insider_purchases_last_6m": pl.Utf8,  # Usually text like 'Purchases', 'Sales', 'Net Shares Purchased'
        "ticker": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "insider_purchases_last_6m"),
    analysis_date_col=None,
)

YFINANCE_INSIDER_ROSTER_SCHEMA = TableSchema(
    table="yfinance_insider_roster_holders",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "name": pl.Utf8,
        "position": pl.Utf8,
        "url": pl.Utf8,
        "most_recent_transaction": pl.Utf8,
        "latest_transaction_date": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
        "shares_owned_directly": pl.Int64,
        "position_direct_date": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "name", "position", "latest_transaction_date"),
    analysis_date_col="latest_transaction_date",
)

# --------------------------------------------------------------------------
# Dataset, Schema Mapper
# --------------------------------------------------------------------------

# Mapping: dataset name -> DuckDB table name
DATASET_TABLE: Final[dict[str, str]] = {
    # Meta
    "info": "yfinance_info",
    "fast_info": "yfinance_fast_info",
    # Market
    "price": "yfinance_price",
    # Actions
    "dividends": "yfinance_dividends",
    "splits": "yfinance_splits",
    "actions": "yfinance_actions",
    # financials
    "financials": "yfinance_financials",
    "income_stmt": "yfinance_financials",
    "balance_sheet": "yfinance_financials",
    "cashflow": "yfinance_financials",
    "earnings": "yfinance_financials",
    "quarterly_financials": "yfinance_financials",
    "quarterly_balance_sheet": "yfinance_financials",
    "quarterly_cashflow": "yfinance_financials",
    "quarterly_earnings": "yfinance_financials",
    # Holders
    "major_holders": "yfinance_major_holders",
    "institutional_holders": "yfinance_holders",
    "mutualfund_holders": "yfinance_holders",
    # Insider
    "insider_roster_holders": "yfinance_insider_roster_holders",
    "insider_purchases": "yfinance_insider_purchases",
    # Analyst
    "recommendations": "yfinance_recommendations",
    # Calendar
    "calendar": "yfinance_calendar",
    # News
    "news": "yfinance_news",
}

# Table Schemas
DATASET_SCHEMA: Final[dict[str, TableSchema]] = {
    # meta
    "info": YFINANCE_INFO_SCHEMA,
    "fast_info": YFINANCE_FAST_INFO_SCHEMA,
    # market
    "price": YFINANCE_PRICE_SCHEMA,
    # Actions
    "dividends": YFINANCE_DIVIDENDS_SCHEMA,
    "splits": YFINANCE_SPLITS_SCHEMA,
    "actions": YFINANCE_ACTIONS_SCHEMA,
    # Holders
    "major_holders": YFINANCE_MAJOR_HOLDERS_SCHEMA,
    "institutional_holders": YFINANCE_HOLDERS_SCHEMA,
    "mutualfund_holders": YFINANCE_HOLDERS_SCHEMA,
    # Insider
    "insider_roster_holders": YFINANCE_INSIDER_ROSTER_SCHEMA,
    "insider_purchases": YFINANCE_INSIDER_PURCHASES_SCHEMA,
    # Financials
    "financials": YFINANCE_FINANCIALS_SCHEMA,
    "income_stmt": YFINANCE_FINANCIALS_SCHEMA,
    "balance_sheet": YFINANCE_FINANCIALS_SCHEMA,
    "cashflow": YFINANCE_FINANCIALS_SCHEMA,
    "earnings": YFINANCE_FINANCIALS_SCHEMA,
    "quarterly_financials": YFINANCE_FINANCIALS_SCHEMA,
    "quarterly_balance_sheet": YFINANCE_FINANCIALS_SCHEMA,
    "quarterly_cashflow": YFINANCE_FINANCIALS_SCHEMA,
    "quarterly_earnings": YFINANCE_FINANCIALS_SCHEMA,
    # Calendar
    "calendar": YFINANCE_CALENDAR_SCHEMA,
    # Analyst
    "recommendations": YFINANCE_RECOMMENDATIONS_SCHEMA,
    # News
    "news": YFINANCE_NEWS_SCHEMA,
}


# --------------------------------------------------------------------------
# Date Filter Column Mapper
# --------------------------------------------------------------------------

INTERNAL_COLS: Final[set[str]] = {"provider", "fetched_at"}

TABLES: Final[dict[str, TableSchema]] = {
    t.table: t
    for t in [
        YFINANCE_PRICE_SCHEMA,
        YFINANCE_INFO_SCHEMA,
        YFINANCE_FAST_INFO_SCHEMA,
        YFINANCE_DIVIDENDS_SCHEMA,
        YFINANCE_SPLITS_SCHEMA,
        YFINANCE_ACTIONS_SCHEMA,
        YFINANCE_CALENDAR_SCHEMA,
        YFINANCE_RECOMMENDATIONS_SCHEMA,
        YFINANCE_NEWS_SCHEMA,
        YFINANCE_FINANCIALS_SCHEMA,
        YFINANCE_HOLDERS_SCHEMA,
        YFINANCE_MAJOR_HOLDERS_SCHEMA,
        YFINANCE_INSIDER_ROSTER_SCHEMA,
        YFINANCE_INSIDER_PURCHASES_SCHEMA,
    ]
}


def _validate_mappings() -> None:
    keys_table = set(DATASET_TABLE.keys())
    keys_schema = set(DATASET_SCHEMA.keys())
    keys_endpoint = set(DATASET_ENDPOINT.keys())
    keys_date_filter = set(DATE_FILTER_COL.keys())
    union = set().union(keys_table, keys_schema, keys_endpoint)
    errors: list[str] = []
    for name, keys in [
        ("DATASET_TABLE", keys_table),
        ("DATASET_SCHEMA", keys_schema),
        ("DATASET_ENDPOINT", keys_endpoint),
    ]:
        if keys != union:
            missing = sorted(union - keys)
            extra = sorted(keys - union)
            errors.append(f"{name}: missing={missing}, extra={extra}")
    if not keys_date_filter.issubset(union):
        missing = sorted(keys_date_filter - union)
        errors.append(f"DATE_FILTER_COL must be subset of datasets; extra={missing}")
    if errors:
        raise RuntimeError("YFinance schema mapping keys inconsistent: " + "; ".join(errors))


_validate_mappings()
