from __future__ import annotations

from typing import Final

import polars as pl

from vertex_forager.schema.config import TableSchema
from vertex_forager.constants import DEFAULT_TIME_ZONE
from vertex_forager.providers.sharadar.constants import (
    DATASET_ENDPOINT,
    DATE_FILTER_COL,
)


SHARADAR_SEP: Final[TableSchema] = TableSchema(
    table="sharadar_sep",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "closeadj": pl.Float64,
        "closeunadj": pl.Float64,
        "lastupdated": pl.Date,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
)


SHARADAR_TICKERS: Final[TableSchema] = TableSchema(
    table="sharadar_tickers",
    schema={
        "provider": pl.Utf8,
        "table": pl.Utf8,
        "permaticker": pl.Utf8,
        "ticker": pl.Utf8,
        "name": pl.Utf8,
        "exchange": pl.Utf8,
        "isdelisted": pl.Utf8,
        "category": pl.Utf8,
        "cusips": pl.Utf8,
        "siccode": pl.Utf8,
        "sicsector": pl.Utf8,
        "sicindustry": pl.Utf8,
        "famasector": pl.Utf8,
        "famaindustry": pl.Utf8,
        "sector": pl.Utf8,
        "industry": pl.Utf8,
        "scalemarketcap": pl.Utf8,
        "scalerevenue": pl.Utf8,
        "relatedtickers": pl.Utf8,
        "currency": pl.Utf8,
        "location": pl.Utf8,
        "companysite": pl.Utf8,
        "secfilings": pl.Utf8,
        "lastupdated": pl.Date,
        "firstadded": pl.Date,
        "firstpricedate": pl.Date,
        "lastpricedate": pl.Date,
        "firstquarter": pl.Date,
        "lastquarter": pl.Date,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker"),
    analysis_date_col=None,
)


SHARADAR_SF1: Final[TableSchema] = TableSchema(
    table="sharadar_sf1",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "dimension": pl.Utf8,
        "calendardate": pl.Date,
        "datekey": pl.Date,
        "reportperiod": pl.Date,
        "fiscalperiod": pl.Utf8,
        "lastupdated": pl.Date,
        "accoci": pl.Int64,
        "assets": pl.Int64,
        "assetsavg": pl.Int64,
        "assetsc": pl.Int64,
        "assetsnc": pl.Int64,
        "assetturnover": pl.Float64,
        "bvps": pl.Float64,
        "capex": pl.Int64,
        "cashneq": pl.Int64,
        "cashnequsd": pl.Int64,
        "cor": pl.Int64,
        "consolinc": pl.Int64,
        "currentratio": pl.Float64,
        "de": pl.Float64,
        "debt": pl.Int64,
        "debtc": pl.Int64,
        "debtnc": pl.Int64,
        "debtusd": pl.Int64,
        "deferredrev": pl.Int64,
        "depamor": pl.Int64,
        "deposits": pl.Int64,
        "divyield": pl.Float64,
        "dps": pl.Float64,
        "ebit": pl.Int64,
        "ebitda": pl.Int64,
        "ebitdamargin": pl.Float64,
        "ebitdausd": pl.Int64,
        "ebitusd": pl.Int64,
        "ebt": pl.Int64,
        "equity": pl.Int64,
        "equityavg": pl.Int64,
        "equityusd": pl.Int64,
        "eps": pl.Float64,
        "epsdil": pl.Float64,
        "epsusd": pl.Float64,
        "ev": pl.Float64,
        "evebit": pl.Float64,
        "evebitda": pl.Float64,
        "fcf": pl.Int64,
        "fcfps": pl.Float64,
        "fxusd": pl.Float64,
        "gp": pl.Int64,
        "grossmargin": pl.Float64,
        "intangibles": pl.Int64,
        "intexp": pl.Int64,
        "inventory": pl.Int64,
        "invcap": pl.Int64,
        "invcapavg": pl.Int64,
        "investments": pl.Int64,
        "investmentsc": pl.Int64,
        "investmentsnc": pl.Int64,
        "liabilities": pl.Int64,
        "liabilitiesc": pl.Int64,
        "liabilitiesnc": pl.Int64,
        "marketcap": pl.Float64,
        "ncf": pl.Int64,
        "ncfbus": pl.Int64,
        "ncfcommon": pl.Int64,
        "ncfdebt": pl.Int64,
        "ncfdiv": pl.Int64,
        "ncff": pl.Int64,
        "ncfi": pl.Int64,
        "ncfinv": pl.Int64,
        "ncfo": pl.Int64,
        "ncfx": pl.Int64,
        "netinc": pl.Int64,
        "netinccmn": pl.Int64,
        "netinccmnusd": pl.Int64,
        "netincdis": pl.Int64,
        "netincnci": pl.Int64,
        "netmargin": pl.Float64,
        "opex": pl.Int64,
        "opinc": pl.Int64,
        "payables": pl.Int64,
        "payoutratio": pl.Float64,
        "pb": pl.Float64,
        "pe": pl.Float64,
        "pe1": pl.Float64,
        "ppnenet": pl.Int64,
        "prefdivis": pl.Int64,
        "price": pl.Float64,
        "ps": pl.Float64,
        "ps1": pl.Float64,
        "receivables": pl.Int64,
        "retearn": pl.Int64,
        "revenue": pl.Int64,
        "revenueusd": pl.Int64,
        "rnd": pl.Int64,
        "roa": pl.Float64,
        "roe": pl.Float64,
        "roic": pl.Float64,
        "ros": pl.Float64,
        "sbcomp": pl.Int64,
        "sgna": pl.Int64,
        "sharefactor": pl.Float64,
        "sharesbas": pl.Int64,
        "shareswa": pl.Int64,
        "shareswadil": pl.Int64,
        "sps": pl.Float64,
        "tangibles": pl.Int64,
        "taxassets": pl.Int64,
        "taxexp": pl.Int64,
        "taxliabilities": pl.Int64,
        "tbvps": pl.Float64,
        "workingcapital": pl.Int64,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "dimension", "calendardate", "reportperiod"),
    analysis_date_col="datekey",
)


SHARADAR_SF2: Final[TableSchema] = TableSchema(
    table="sharadar_sf2",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "filingdate": pl.Date,
        "formtype": pl.Utf8,
        "issuername": pl.Utf8,
        "ownername": pl.Utf8,
        "officertitle": pl.Utf8,
        "isdirector": pl.Utf8,
        "isofficer": pl.Utf8,
        "istenpercentowner": pl.Utf8,
        "transactiondate": pl.Date,
        "securityadcode": pl.Utf8,
        "transactioncode": pl.Utf8,
        "transactionshares": pl.Int64,
        "transactionpricepershare": pl.Float64,
        "transactionvalue": pl.Float64,
        "sharesownedbeforetransaction": pl.Int64,
        "sharesownedfollowingtransaction": pl.Int64,
        "directorindirect": pl.Utf8,
        "natureofownership": pl.Utf8,
        "dateexercisable": pl.Date,
        "priceexercisable": pl.Float64,
        "expirationdate": pl.Date,
        "rownum": pl.Int64,
        "securitytitle": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "filingdate", "rownum"),
    analysis_date_col="filingdate",
)


SHARADAR_SF3: Final[TableSchema] = TableSchema(
    table="sharadar_sf3",
    schema={
        "provider": pl.Utf8,
        "ticker": pl.Utf8,
        "calendardate": pl.Date,
        "investorname": pl.Utf8,
        "securitytype": pl.Utf8,
        "units": pl.Int64,
        "price": pl.Float64,
        "value": pl.Float64,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "calendardate", "investorname", "securitytype"),
    analysis_date_col="calendardate",
)


SHARADAR_ACTIONS: Final[TableSchema] = TableSchema(
    table="sharadar_actions",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "action": pl.Utf8,
        "ticker": pl.Utf8,
        "name": pl.Utf8,
        "value": pl.Float64,
        "contraticker": pl.Utf8,
        "contraname": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date", "action"),
    analysis_date_col="date",
)


SHARADAR_DAILY: Final[TableSchema] = TableSchema(
    table="sharadar_daily",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "ticker": pl.Utf8,
        "marketcap": pl.Float64,
        "ev": pl.Float64,
        "pe": pl.Float64,
        "pb": pl.Float64,
        "ps": pl.Float64,
        "evebit": pl.Float64,
        "evebitda": pl.Float64,
        "lastupdated": pl.Date,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date"),
    analysis_date_col="date",
)


SHARADAR_SP500: Final[TableSchema] = TableSchema(
    table="sharadar_sp500",
    schema={
        "provider": pl.Utf8,
        "date": pl.Date,
        "action": pl.Utf8,
        "ticker": pl.Utf8,
        "name": pl.Utf8,
        "contraticker": pl.Utf8,
        "contraname": pl.Utf8,
        "note": pl.Utf8,
        "fetched_at": pl.Datetime(time_zone=DEFAULT_TIME_ZONE),
    },
    unique_key=("provider", "ticker", "date", "action"),
    analysis_date_col="date",
)


TABLES: Final[dict[str, TableSchema]] = {
    t.table: t
    for t in [
        SHARADAR_SEP,
        SHARADAR_TICKERS,
        SHARADAR_SF1,
        SHARADAR_SF2,
        SHARADAR_SF3,
        SHARADAR_ACTIONS,
        SHARADAR_DAILY,
        SHARADAR_SP500,
    ]
}

# Dataset→Table name mapping (used by Router/Client)
DATASET_TABLE: Final[dict[str, str]] = {
    "price": "sharadar_sep",
    "tickers": "sharadar_tickers",
    "fundamental": "sharadar_sf1",
    "daily": "sharadar_daily",
    "actions": "sharadar_actions",
    "insider": "sharadar_sf2",
    "institutional": "sharadar_sf3",
    "sp500": "sharadar_sp500",
}

# Dataset→Schema mapping
DATASET_SCHEMA: Final[dict[str, TableSchema]] = {
    "price": SHARADAR_SEP,
    "tickers": SHARADAR_TICKERS,
    "fundamental": SHARADAR_SF1,
    "insider": SHARADAR_SF2,
    "institutional": SHARADAR_SF3,
    "actions": SHARADAR_ACTIONS,
    "daily": SHARADAR_DAILY,
    "sp500": SHARADAR_SP500,
}

# Endpoint mapping for provider API

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
        raise RuntimeError(
            "Sharadar schema mapping keys inconsistent: " + "; ".join(errors)
        )

_validate_mappings()
