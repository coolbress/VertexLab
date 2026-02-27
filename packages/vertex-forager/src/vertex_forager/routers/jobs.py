from __future__ import annotations

from typing import Mapping, Any

from vertex_forager.core.config import FetchJob, RequestSpec, HttpMethod


def make_pagination_context(
    *,
    meta_key: str = "next_cursor_id",
    cursor_param: str = "qopts.cursor_id",
    max_pages: int | None = 1000,
) -> dict[str, Any]:
    """Create a standardized pagination context dict."""
    ctx: dict[str, Any] = {
        "pagination": {
            "cursor_param": cursor_param,
            "meta_key": meta_key,
        }
    }
    if max_pages is not None:
        ctx["pagination"]["max_pages"] = max_pages
    return ctx


def pagination_job(
    *,
    provider: str,
    dataset: str,
    url: str,
    params: Mapping[str, Any],
    auth: Any,
    context: dict[str, Any],
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
    params: Mapping[str, Any],
    auth: Any | None = None,
    context: dict[str, Any] | None = None,
) -> FetchJob:
    """Create a per-symbol FetchJob."""
    if auth is None:
        spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params))
    else:
        spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params), auth=auth)
    return FetchJob(provider=provider, dataset=dataset, symbol=symbol, spec=spec, context=context or {})
