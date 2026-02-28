from __future__ import annotations

from typing import Mapping, Any
from vertex_forager.core.types import JSONValue, PaginationParams, PaginationJobContext, PerSymbolJobContext

from vertex_forager.core.config import FetchJob, RequestSpec, HttpMethod


def make_pagination_context(
    *,
    meta_key: str = "next_cursor_id",
    cursor_param: str = "qopts.cursor_id",
    max_pages: int | None = 1000,
) -> PaginationJobContext:
    """Create a standardized pagination context dict."""
    pagination: PaginationParams = {"cursor_param": cursor_param, "meta_key": meta_key}
    if max_pages is not None:
        pagination["max_pages"] = int(max_pages)
    return {"pagination": pagination}

def build_symbol_context(*, dataset: str, symbol: str) -> PerSymbolJobContext:
    """Standardized builder for per-symbol job context."""
    return {"dataset": dataset, "symbol": symbol}

def pagination_job(
    *,
    provider: str,
    dataset: str,
    url: str,
    params: Mapping[str, JSONValue],
    auth: Any,
    context: PaginationJobContext,
) -> FetchJob:
    """Create a pagination-aware FetchJob."""
    spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params), auth=auth)
    return FetchJob(provider=provider, dataset=dataset, symbol=None, spec=spec, context=context)


def single_symbol_job(
    *,
    provider: str,
    dataset: str,
    symbol: str,
    url: str,
    params: Mapping[str, JSONValue],
    auth: Any | None = None,
    context: PerSymbolJobContext | None = None,
) -> FetchJob:
    """Create a per-symbol FetchJob."""
    if auth is None:
        spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params))
    else:
        spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params), auth=auth)
    return FetchJob(provider=provider, dataset=dataset, symbol=symbol, spec=spec, context=context or {})
