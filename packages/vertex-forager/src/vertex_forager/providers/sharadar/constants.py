"""Sharadar provider constants."""

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
ESTIMATED_TOTAL_TICKERS: Final[int] = 50_000

API_KEY_QUERY_PARAM: Final[str] = "api_key"
QOPTS_PER_PAGE: Final[str] = "qopts.per_page"
QOPTS_COLUMNS: Final[str] = "qopts.columns"
INTERNAL_COLS: Final[set[str]] = {"provider", "fetched_at"}
