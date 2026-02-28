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
