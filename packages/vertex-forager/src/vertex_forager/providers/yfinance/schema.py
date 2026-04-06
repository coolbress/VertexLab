"""YFinance schema definitions for vertex_forager provider."""

from __future__ import annotations

from typing import Final

import polars as pl

from vertex_forager.constants import DEFAULT_TIME_ZONE
from vertex_forager.core.quality import NoDuplicateRows, NoFutureDates, NoNegativePrices
from vertex_forager.schema.config import DatasetSpec, TableSchema

YFINANCE_INFO_SCHEMA = TableSchema(
    table="yfinance_info",
    schema={
        "provider": pl.String,
        "ticker": pl.String,
        "shortname": pl.String,
        "longname": pl.String,
        "currency": pl.String,
        "exchange": pl.String,
        "sector": pl.String,
        "industry": pl.String,
        "website": pl.String,
        "address1": pl.String,
        "city": pl.String,
        "state": pl.String,
        "zip": pl.String,
        "country": pl.String,
        "phone": pl.String,
        "language": pl.String,
        "region": pl.String,
        "quotesourcename": pl.String,
        "displayname": pl.String,
        "marketcap": pl.Int64,
        "longbusinesssummary": pl.String,
        "fulltimeemployees": pl.Int64,
        "irwebsite": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker"),
    analysis_date_col=None,
)

YFINANCE_PRICE_SCHEMA = TableSchema(
    table="yfinance_price",
    schema={
        "provider": pl.String,
        "date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "adj_close": pl.Float64,
        "volume": pl.Float64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
    quality_rules=(
        NoFutureDates(["date"]),
        NoNegativePrices(["open", "high", "low", "close", "adj_close"]),
        NoDuplicateRows(["ticker", "date"]),
    ),
)

YFINANCE_DIVIDENDS_SCHEMA = TableSchema(
    table="yfinance_dividends",
    schema={
        "provider": pl.String,
        "date": pl.Date,
        "dividends": pl.Float64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
    quality_rules=(
        NoFutureDates(["date"]),
        NoDuplicateRows(["date", "ticker", "provider"]),
    ),
)

YFINANCE_SPLITS_SCHEMA = TableSchema(
    table="yfinance_splits",
    schema={
        "provider": pl.String,
        "date": pl.Date,
        "stock_splits": pl.Float64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
    quality_rules=(
        NoFutureDates(["date"]),
        NoDuplicateRows(["date", "ticker", "provider"]),
    ),
)

YFINANCE_ACTIONS_SCHEMA = TableSchema(
    table="yfinance_actions",
    schema={
        "provider": pl.String,
        "date": pl.Date,
        "dividends": pl.Float64,
        "stock_splits": pl.Float64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
    quality_rules=(
        NoFutureDates(["date"]),
        NoDuplicateRows(["ticker", "date"]),
    ),
)

YFINANCE_CALENDAR_SCHEMA = TableSchema(
    table="yfinance_calendar",
    schema={
        "provider": pl.String,
        "earnings_date": pl.Date,
        "earnings_average": pl.Float64,
        "earnings_low": pl.Float64,
        "earnings_high": pl.Float64,
        "revenue_average": pl.Int64,
        "revenue_low": pl.Int64,
        "revenue_high": pl.Int64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "earnings_date"),
    analysis_date_col="earnings_date",
)

YFINANCE_RECOMMENDATIONS_SCHEMA = TableSchema(
    table="yfinance_recommendations",
    schema={
        "provider": pl.String,
        "period": pl.String,
        "strongbuy": pl.Int64,
        "buy": pl.Int64,
        "hold": pl.Int64,
        "sell": pl.Int64,
        "strongsell": pl.Int64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "period"),
    analysis_date_col=None,
)

YFINANCE_NEWS_SCHEMA = TableSchema(
    table="yfinance_news",
    schema={
        "provider": pl.String,
        "ticker": pl.String,
        "id": pl.String,
        "title": pl.String,
        "publisher": pl.String,
        "type": pl.String,
        "link": pl.String,
        "published_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "id", "published_at"),
    analysis_date_col="published_at",
)

YFINANCE_FINANCIALS_SCHEMA = TableSchema(
    table="yfinance_financials",
    schema={
        "date": pl.Date,
        "ticker": pl.String,
        "provider": pl.String,
        "period": pl.String,
        "metric": pl.String,
        "value": pl.Float64,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("date", "ticker", "provider", "period", "metric"),
    analysis_date_col="date",
    quality_rules=(
        NoFutureDates(["date"]),
        NoDuplicateRows(["date", "ticker", "provider", "period", "metric"]),
    ),
)

YFINANCE_HOLDERS_SCHEMA = TableSchema(
    table="yfinance_holders",
    schema={
        "provider": pl.String,
        "holder": pl.String,
        "shares": pl.Float64,
        "date_reported": pl.Date,
        "percentage_out": pl.Float64,
        "pctheld": pl.Float64,
        "pctchange": pl.Float64,
        "value": pl.Float64,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "holder", "date_reported"),
    analysis_date_col="date_reported",
    quality_rules=(NoFutureDates(["date_reported"]),),
)

YFINANCE_FAST_INFO_SCHEMA = TableSchema(
    table="yfinance_fast_info",
    schema={
        "provider": pl.String,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker"),
    analysis_date_col=None,
    flexible_schema=True,
)

YFINANCE_MAJOR_HOLDERS_SCHEMA = TableSchema(
    table="yfinance_major_holders",
    schema={
        "provider": pl.String,
        "ticker": pl.String,
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
        "provider": pl.String,
        "holder": pl.String,
        "shares": pl.Float64,
        "trans": pl.Int64,
        "insider_purchases_last_6m": pl.String,
        "ticker": pl.String,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "insider_purchases_last_6m"),
    analysis_date_col=None,
)

YFINANCE_INSIDER_ROSTER_SCHEMA = TableSchema(
    table="yfinance_insider_roster_holders",
    schema={
        "provider": pl.String,
        "ticker": pl.String,
        "name": pl.String,
        "position": pl.String,
        "url": pl.String,
        "most_recent_transaction": pl.String,
        "latest_transaction_date": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
        "shares_owned_directly": pl.Int64,
        "position_direct_date": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "name", "position", "latest_transaction_date"),
    analysis_date_col="latest_transaction_date",
    quality_rules=(NoFutureDates(["latest_transaction_date"]),),
)

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

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    DatasetSpec(name="info", schema=YFINANCE_INFO_SCHEMA, endpoint="info"),
    DatasetSpec(name="fast_info", schema=YFINANCE_FAST_INFO_SCHEMA, endpoint="fast_info"),
    DatasetSpec(name="price", schema=YFINANCE_PRICE_SCHEMA, endpoint="history", date_filter_col="date"),
    DatasetSpec(name="dividends", schema=YFINANCE_DIVIDENDS_SCHEMA, endpoint="dividends", date_filter_col="date"),
    DatasetSpec(name="splits", schema=YFINANCE_SPLITS_SCHEMA, endpoint="splits", date_filter_col="date"),
    DatasetSpec(name="actions", schema=YFINANCE_ACTIONS_SCHEMA, endpoint="actions"),
    DatasetSpec(name="financials", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="financials", date_filter_col="date"),
    DatasetSpec(name="income_stmt", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="financials"),
    DatasetSpec(
        name="balance_sheet", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="balance_sheet", date_filter_col="date"
    ),
    DatasetSpec(name="cashflow", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="cashflow", date_filter_col="date"),
    DatasetSpec(name="earnings", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="earnings", date_filter_col="date"),
    DatasetSpec(name="quarterly_financials", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="quarterly_financials"),
    DatasetSpec(
        name="quarterly_balance_sheet",
        schema=YFINANCE_FINANCIALS_SCHEMA,
        endpoint="quarterly_balance_sheet",
    ),
    DatasetSpec(name="quarterly_cashflow", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="quarterly_cashflow"),
    DatasetSpec(name="quarterly_earnings", schema=YFINANCE_FINANCIALS_SCHEMA, endpoint="quarterly_earnings"),
    DatasetSpec(name="major_holders", schema=YFINANCE_MAJOR_HOLDERS_SCHEMA, endpoint="major_holders"),
    DatasetSpec(name="institutional_holders", schema=YFINANCE_HOLDERS_SCHEMA, endpoint="institutional_holders"),
    DatasetSpec(name="mutualfund_holders", schema=YFINANCE_HOLDERS_SCHEMA, endpoint="mutualfund_holders"),
    DatasetSpec(
        name="insider_roster_holders",
        schema=YFINANCE_INSIDER_ROSTER_SCHEMA,
        endpoint="insider_roster_holders",
    ),
    DatasetSpec(name="insider_purchases", schema=YFINANCE_INSIDER_PURCHASES_SCHEMA, endpoint="insider_purchases"),
    DatasetSpec(name="recommendations", schema=YFINANCE_RECOMMENDATIONS_SCHEMA, endpoint="recommendations"),
    DatasetSpec(name="calendar", schema=YFINANCE_CALENDAR_SCHEMA, endpoint="calendar"),
    DatasetSpec(name="news", schema=YFINANCE_NEWS_SCHEMA, endpoint="news", date_filter_col="published_at"),
)

DATASET_SPECS: Final[dict[str, DatasetSpec]] = {spec.name: spec for spec in DATASETS}
DATASET_NAMES: Final[tuple[str, ...]] = tuple(spec.name for spec in DATASETS)
