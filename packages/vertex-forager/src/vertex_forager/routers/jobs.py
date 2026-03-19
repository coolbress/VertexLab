from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from vertex_forager.core.config import FetchJob, HttpMethod, RequestSpec

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertex_forager.core.types import JSONValue, PaginationJobContext, PaginationParams, PerSymbolJobContext


def make_pagination_context(
    *,
    dataset: str,
    meta_key: str = "next_cursor_id",
    cursor_param: str = "qopts.cursor_id",
    max_pages: int | None = 1000,
) -> PaginationJobContext:
    """Create a standardized pagination context dictionary.

    Args:
        dataset: Dataset name associated with pagination (e.g., "tickers").
        meta_key: Metadata key in provider response that contains the next cursor id.
            Defaults to "next_cursor_id".
        cursor_param: Request parameter name used to pass the cursor id.
            Defaults to "qopts.cursor_id".
        max_pages: Optional cap on total pagination cycles. If None, pagination
            is unbounded. Defaults to 1000.

    Returns:
        PaginationJobContext: A mapping containing the 'pagination' config and
        the originating 'dataset' for downstream correlation.
    """
    pagination: PaginationParams = {"cursor_param": cursor_param, "meta_key": meta_key}
    if max_pages is not None:
        pagination["max_pages"] = int(max_pages)
    return {"pagination": pagination, "dataset": dataset}


def build_symbol_context(*, dataset: str, symbol: str) -> PerSymbolJobContext:
    """Build standardized context for per-symbol jobs.

    Args:
        dataset: Dataset name (e.g., "price").
        symbol: Target ticker symbol (or comma-separated list).

    Returns:
        PerSymbolJobContext: Context with 'dataset' and 'symbol' for downstream use.
    """
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
    """Create a pagination-aware FetchJob.

    Args:
        provider: Provider identifier (e.g., "sharadar").
        dataset: Dataset name for the job.
        url: Target URL for the request.
        params: Request parameters (JSONValue-safe mapping).
        auth: Authentication configuration (attached to RequestSpec).
        context: Pagination context including cursor param, meta key, and optional max pages.

    Returns:
        FetchJob: Constructed job with pagination context.
    """
    spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params), auth=auth)
    return FetchJob(
        provider=provider,
        dataset=dataset,
        symbol=None,
        spec=spec,
        context=cast("Mapping[str, JSONValue]", context),
    )


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
    """Create a per-symbol FetchJob.

    Args:
        provider: Provider identifier (e.g., "yfinance").
        dataset: Dataset name for the job.
        symbol: Target ticker symbol (or comma-separated list).
        url: Target URL for the request.
        params: Request parameters (JSONValue-safe mapping).
        auth: Optional authentication configuration.
        context: Optional job context; must include 'dataset' and 'symbol'. If missing,
            they are injected via a standardized builder while preserving extra keys.

    Returns:
        FetchJob: Constructed job with symbol context.
    """
    if auth is None:
        spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params))
    else:
        spec = RequestSpec(method=HttpMethod.GET, url=url, params=dict(params), auth=auth)

    # Ensure context has required fields if not provided
    final_context: PerSymbolJobContext
    if context is None:
        final_context = build_symbol_context(dataset=dataset, symbol=symbol)
    else:
        # Validate that context has required fields if passed
        if "dataset" not in context or "symbol" not in context:
            # Fallback or merge - for safety we rebuild base context and merge extras
            base = build_symbol_context(dataset=dataset, symbol=symbol)
            # Cast to dict to merge, then cast back
            merged = {**base, **context}
            final_context = cast("PerSymbolJobContext", merged)
        else:
            final_context = context

    return FetchJob(
        provider=provider,
        dataset=dataset,
        symbol=symbol,
        spec=spec,
        context=cast("Mapping[str, JSONValue]", final_context),
    )
