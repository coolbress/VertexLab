"""Sharadar provider constants.

Purpose:
- Centralize dataset endpoints, date filters, batching limits, and request keys
- Improve maintainability and reduce duplication across router/schema/client
"""
from __future__ import annotations

from typing import Final

MAX_ROWS_PER_REQUEST: Final[int] = 10000
DEFAULT_BATCH_SIZE: Final[int] = 100
MIN_BATCH_SIZE: Final[int] = 1
TRADING_DAYS_RATIO: Final[float] = 0.7
QUARTERLY_DAYS_RATIO: Final[float] = 1 / 90

PAGINATION_META_KEY: Final[str] = "next_cursor_id"
PAGINATION_CURSOR_PARAM: Final[str] = "qopts.cursor_id"
MAX_PAGES: Final[int] = 1000

BYTES_PER_TICKER_METADATA: Final[int] = 1024
BYTES_PER_TICKER_FULL: Final[int] = 1 * 1024 * 1024
ESTIMATED_TOTAL_TICKERS: Final[int] = 15_000

API_KEY_QUERY_PARAM: Final[str] = "api_key"
QOPTS_PER_PAGE: Final[str] = "qopts.per_page"
QOPTS_COLUMNS: Final[str] = "qopts.columns"
DATASET_ENDPOINT: Final[dict[str, str]] = {
    "price": "SEP",
    "fundamental": "SF1",
    "daily": "DAILY",
    "tickers": "TICKERS",
    "actions": "ACTIONS",
    "insider": "SF2",
    "institutional": "SF3",
    "sp500": "SP500",
}

DATE_FILTER_COL: Final[dict[str, str]] = {
    "price": "date",
    "fundamental": "calendardate",
    "daily": "date",
    "actions": "date",
    "insider": "filingdate",
    "institutional": "calendardate",
    "sp500": "date",
}

INTERNAL_COLS: Final[set[str]] = {"provider", "fetched_at"}
