from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vertex_forager.core.config import RequestSpec
    from vertex_forager.core.types import JSONValue


def _parse_spec(spec: RequestSpec) -> tuple[str, str, dict[str, JSONValue]]:
    if "://" not in spec.url:
        raise ValueError("Library URL must contain scheme separator '://'")
    scheme, payload = spec.url.split("://", 1)
    if scheme != "yfinance":
        raise ValueError(f"Unsupported library scheme: {scheme}")
    params = spec.params
    dataset = params.get("dataset", "price")
    lib = params.get("lib")
    if not isinstance(lib, dict):
        raise ValueError("Missing library call specification ('lib') in request params")
    return payload, str(dataset), dict(lib)


def fetch_yfinance(spec: RequestSpec, *, yf_lib: Any) -> Any:
    """Execute a yfinance library call described by RequestSpec.

    This is the provider-specific library path used by `HttpExecutor` for the
    `yfinance://` scheme.
    """
    if yf_lib is None:
        raise ValueError("yfinance library not available")
    ticker_symbol, dataset, lib = _parse_spec(spec)
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
