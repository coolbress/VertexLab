from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import vertex_forager.core.http as _http_mod
from vertex_forager.core.library import (
    BaseLibraryFetcher,
    get_library_fetcher,
    register_library_fetcher,
)

if TYPE_CHECKING:
    from vertex_forager.core.config import RequestSpec
    from vertex_forager.core.types import JSONValue


class YFinanceLibraryFetcher(BaseLibraryFetcher):
    """YFinance library fetcher.

    Provides scheme-based dispatch for yfinance calls and integrates with the
    library fetcher registry via `scheme`.
    """

    scheme = "yfinance"

    def fetch(self, spec: RequestSpec) -> Any:
        """Execute a yfinance library call described by RequestSpec.

        Args:
            spec: Request specification containing URL, params with `lib`.

        Returns:
            Any: Result from yfinance APIs (download result or attribute call).

        Raises:
            ValueError: When yfinance is unavailable or call specification invalid.
        """
        yf_lib = cast("Any", getattr(_http_mod, "yf", None))
        if yf_lib is None:
            raise ValueError("yfinance library not available")
        ticker_symbol, dataset, lib = self.parse_spec(spec)
        call_type = lib.get("type")
        kw = lib.get("kwargs")
        call_kwargs: dict[str, JSONValue] = dict(kw) if isinstance(kw, dict) else {}
        if call_type == "download":
            return yf_lib.download(tickers=ticker_symbol, **call_kwargs)
        if call_type == "ticker_attr":
            attr_name = lib.get("attr")
            if not isinstance(attr_name, str) or attr_name.startswith("_") or "__" in attr_name:
                raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}")
            ticker = yf_lib.Ticker(ticker_symbol)
            try:
                attr = getattr(ticker, attr_name)
            except AttributeError:
                raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}") from None
            return attr(**call_kwargs) if callable(attr) else attr
        raise ValueError(f"Unsupported library call type: {call_type}")


_existing = get_library_fetcher("yfinance")
if _existing is None:
    register_library_fetcher(YFinanceLibraryFetcher())
else:
    if not isinstance(_existing, YFinanceLibraryFetcher):
        raise ValueError("Library fetcher scheme conflict for 'yfinance'")
