from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vertex_forager.core.config import RequestSpec
    from vertex_forager.core.types import JSONValue

TICKER_ATTR_ALLOWLIST: dict[str, frozenset[str]] = {
    "actions": frozenset({"actions"}),
    "balance_sheet": frozenset({"balance_sheet"}),
    "calendar": frozenset({"calendar"}),
    "cashflow": frozenset({"cashflow"}),
    "dividends": frozenset({"dividends"}),
    "earnings": frozenset({"earnings"}),
    "fast_info": frozenset({"fast_info"}),
    "financials": frozenset({"financials"}),
    "info": frozenset({"info"}),
    "insider_purchases": frozenset({"insider_purchases"}),
    "insider_roster_holders": frozenset({"insider_roster_holders"}),
    "institutional_holders": frozenset({"institutional_holders"}),
    "major_holders": frozenset({"major_holders"}),
    "mutualfund_holders": frozenset({"mutualfund_holders"}),
    "news": frozenset({"news"}),
    "price": frozenset({"history"}),
    "quarterly_balance_sheet": frozenset({"quarterly_balance_sheet"}),
    "quarterly_cashflow": frozenset({"quarterly_cashflow"}),
    "quarterly_financials": frozenset({"quarterly_financials"}),
    "recommendations": frozenset({"recommendations"}),
    "splits": frozenset({"splits"}),
}


def _parse_spec(spec: RequestSpec) -> tuple[str, str, dict[str, JSONValue]]:
    if "://" not in spec.url:
        raise ValueError("Library URL must contain scheme separator '://'")
    scheme, payload = spec.url.split("://", 1)
    payload = payload.strip()
    if not payload:
        raise ValueError("Empty library payload in request URL")
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
    if kw is None:
        call_kwargs: dict[str, JSONValue] = {}
    elif isinstance(kw, dict):
        call_kwargs = dict(kw)
    else:
        raise ValueError(
            f"Invalid library kwargs for yfinance dataset '{dataset}': expected dict, got {type(kw).__name__}"
        )
    if call_type == "download":
        return yf_lib.download(tickers=ticker_symbol, **call_kwargs)
    if call_type == "ticker_attr":
        attr_name = lib.get("attr")
        allowed_attrs = TICKER_ATTR_ALLOWLIST.get(dataset)
        if not isinstance(attr_name, str) or allowed_attrs is None or attr_name not in allowed_attrs:
            raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}")
        ticker = yf_lib.Ticker(ticker_symbol)
        try:
            attr = getattr(ticker, attr_name)
        except AttributeError:
            raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}") from None
        if callable(attr):
            return attr(**call_kwargs)
        if call_kwargs:
            raise ValueError(f"Invalid library kwargs for non-callable yfinance dataset '{dataset}' -> {attr_name}")
        return attr
    raise ValueError(f"Unsupported library call type: {call_type}")
