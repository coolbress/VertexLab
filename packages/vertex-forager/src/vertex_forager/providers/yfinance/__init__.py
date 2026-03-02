"""YFinance provider package."""

from typing import TYPE_CHECKING, Any

from vertex_forager.providers.yfinance.schema import (
    YFINANCE_INFO_SCHEMA,
    YFINANCE_FAST_INFO_SCHEMA,
    YFINANCE_PRICE_SCHEMA,
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
)

__import__("vertex_forager.providers.yfinance.fetcher")

if TYPE_CHECKING:
    from vertex_forager.providers.yfinance.client import YFinanceClient
    from vertex_forager.providers.yfinance.router import YFinanceRouter


def __getattr__(name: str) -> Any:
    if name == "YFinanceClient":
        from vertex_forager.providers.yfinance.client import YFinanceClient
        return YFinanceClient
    if name == "YFinanceRouter":
        from vertex_forager.providers.yfinance.router import YFinanceRouter
        return YFinanceRouter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "YFinanceClient",
    "YFinanceRouter",
    "YFINANCE_INFO_SCHEMA",
    "YFINANCE_FAST_INFO_SCHEMA",
    "YFINANCE_PRICE_SCHEMA",
    "YFINANCE_DIVIDENDS_SCHEMA",
    "YFINANCE_SPLITS_SCHEMA",
    "YFINANCE_ACTIONS_SCHEMA",
    "YFINANCE_CALENDAR_SCHEMA",
    "YFINANCE_RECOMMENDATIONS_SCHEMA",
    "YFINANCE_NEWS_SCHEMA",
    "YFINANCE_FINANCIALS_SCHEMA",
    "YFINANCE_HOLDERS_SCHEMA",
    "YFINANCE_MAJOR_HOLDERS_SCHEMA",
    "YFINANCE_INSIDER_ROSTER_SCHEMA",
    "YFINANCE_INSIDER_PURCHASES_SCHEMA",
]
