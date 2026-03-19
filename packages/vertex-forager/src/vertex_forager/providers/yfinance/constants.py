"""YFinance provider constants.

Purpose:
- Centralize dataset mapping, date filters, and batch thresholds
- Improve maintainability and reduce duplication across router/schema/client
"""

from __future__ import annotations

from typing import Final

SIZE_MAP: Final[dict[str, int]] = {
    "info": 30 * 1024,
    "price": 600 * 1024,
    "financials": 200 * 1024,
    "quarterly_financials": 220 * 1024,
    "balance_sheet": 180 * 1024,
    "quarterly_balance_sheet": 200 * 1024,
    "cashflow": 180 * 1024,
    "quarterly_cashflow": 200 * 1024,
    "dividends": 40 * 1024,
    "splits": 40 * 1024,
    "major_holders": 50 * 1024,
    "institutional_holders": 80 * 1024,
    "mutualfund_holders": 80 * 1024,
    "insider_roster_holders": 80 * 1024,
    "insider_purchases": 120 * 1024,
    "recommendations": 100 * 1024,
    "calendar": 24 * 1024,
    "news": 300 * 1024,
}

PRICE_BATCH_SIZE: Final[int] = 250
THREADS_THRESHOLD: Final[int] = 50
PRICE_BATCH_MAX: Final[int] = 500
PRICE_BATCH_SIZE_KEY: Final[str] = "price_batch_size"
DEFAULT_BYTES_PER_ITEM: Final[int] = 200 * 1024

# Dataset Request Endpoint aliases
DATASET_ENDPOINT: Final[dict[str, str]] = {
    "price": "history",
    "info": "info",
    "fast_info": "fast_info",
    "dividends": "dividends",
    "splits": "splits",
    "actions": "actions",
    "financials": "financials",
    "income_stmt": "financials",
    "balance_sheet": "balance_sheet",
    "cashflow": "cashflow",
    "earnings": "earnings",
    "quarterly_financials": "quarterly_financials",
    "quarterly_balance_sheet": "quarterly_balance_sheet",
    "quarterly_cashflow": "quarterly_cashflow",
    "quarterly_earnings": "quarterly_earnings",
    "major_holders": "major_holders",
    "institutional_holders": "institutional_holders",
    "mutualfund_holders": "mutualfund_holders",
    "insider_roster_holders": "insider_roster_holders",
    "insider_purchases": "insider_purchases",
    "recommendations": "recommendations",
    "calendar": "calendar",
    "news": "news",
}

DATE_FILTER_COL: Final[dict[str, str]] = {
    "price": "date",
    "dividends": "date",
    "splits": "date",
    "financials": "date",
    "balance_sheet": "date",
    "cashflow": "date",
    "earnings": "date",
    "recommendations": "period",
    "news": "published_at",
}

# Price request defaults and keys
INTERVAL_KEY: Final[str] = "interval"
START_KEY: Final[str] = "start"
END_KEY: Final[str] = "end"
PERIOD_KEY: Final[str] = "period"
AUTO_ADJUST_KEY: Final[str] = "auto_adjust"
PREPOST_KEY: Final[str] = "prepost"

DEFAULT_INTERVAL: Final[str] = "1d"
DEFAULT_PRICE_PERIOD: Final[str] = "max"
DEFAULT_AUTO_ADJUST: Final[bool] = False
DEFAULT_PREPOST: Final[bool] = False
