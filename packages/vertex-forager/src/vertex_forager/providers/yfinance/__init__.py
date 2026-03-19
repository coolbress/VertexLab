"""YFinance provider package."""

from typing import Any

__import__("vertex_forager.providers.yfinance.fetcher")


def __getattr__(name: str) -> Any:
    if name == "YFinanceClient":
        from vertex_forager.providers.yfinance.client import YFinanceClient

        return YFinanceClient
    if name == "YFinanceRouter":
        from vertex_forager.providers.yfinance.router import YFinanceRouter

        return YFinanceRouter
    if name.startswith("YFINANCE_"):
        from vertex_forager.providers.yfinance import schema as _schema

        try:
            return getattr(_schema, name)
        except AttributeError as e:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from e
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = sorted(
    [
        "YFINANCE_ACTIONS_SCHEMA",
        "YFINANCE_CALENDAR_SCHEMA",
        "YFINANCE_DIVIDENDS_SCHEMA",
        "YFINANCE_SPLITS_SCHEMA",
        "YFINANCE_FAST_INFO_SCHEMA",
        "YFINANCE_FINANCIALS_SCHEMA",
        "YFINANCE_HOLDERS_SCHEMA",
        "YFINANCE_INFO_SCHEMA",
        "YFINANCE_INSIDER_PURCHASES_SCHEMA",
        "YFINANCE_INSIDER_ROSTER_SCHEMA",
        "YFINANCE_MAJOR_HOLDERS_SCHEMA",
        "YFINANCE_NEWS_SCHEMA",
        "YFINANCE_PRICE_SCHEMA",
        "YFINANCE_RECOMMENDATIONS_SCHEMA",
        "YFinanceClient",
        "YFinanceRouter",
    ]
)
