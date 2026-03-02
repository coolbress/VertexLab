from __future__ import annotations

from typing import Any
import vertex_forager.core.http as _http_mod
from vertex_forager.core.config import RequestSpec
from vertex_forager.core.types import JSONValue
from vertex_forager.core.library import register_library_fetcher, BaseLibraryFetcher


class YFinanceLibraryFetcher(BaseLibraryFetcher):
    scheme = "yfinance"

    def fetch(self, spec: RequestSpec) -> Any:
        ticker_symbol, dataset, lib = self.parse_spec(spec)
        call_type = lib.get("type")
        kw = lib.get("kwargs")
        call_kwargs: dict[str, "JSONValue"] = dict(kw) if isinstance(kw, dict) else {}
        if call_type == "download":
            return _http_mod.yf.download(tickers=ticker_symbol, **call_kwargs)
        if call_type == "ticker_attr":
            attr_name = lib.get("attr")
            ticker = _http_mod.yf.Ticker(ticker_symbol)
            if not isinstance(attr_name, str) or not hasattr(ticker, attr_name):
                raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}")
            attr = getattr(ticker, attr_name)
            return attr(**call_kwargs) if callable(attr) else attr
        raise ValueError(f"Unsupported library call type: {call_type}")


register_library_fetcher(YFinanceLibraryFetcher())
