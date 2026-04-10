"""Router implementations for different data sources.

DIP Note:
- Factories return `BaseRouter` implementations to keep provider dispatch explicit
  while leaving routing behavior isolated inside provider modules.
"""

from __future__ import annotations

from typing import Any

from vertex_forager.routers.base import BaseRouter


def _sharadar_factory(**kwargs: Any) -> BaseRouter:
    from vertex_forager.providers.sharadar.router import SharadarRouter

    return SharadarRouter(**kwargs)


def _yfinance_factory(**kwargs: Any) -> BaseRouter:
    from vertex_forager.providers.yfinance.router import YFinanceRouter

    return YFinanceRouter(**kwargs)


def create_router(
    provider: str,
    *,
    api_key: str | None,
    rate_limit: int,
    start_date: str | None = None,
    end_date: str | None = None,
    **kwargs: Any,
) -> BaseRouter:
    """
    Create and configure a router instance for the specified provider.

    Args:
        provider: The provider identifier (e.g., "sharadar").
        api_key: API key.
        rate_limit: Effective requests-per-minute setting for the router.
        start_date: Optional start date filter.
        end_date: Optional end date filter.
        **kwargs: Additional provider-specific configuration.

    Returns:
        Configured router instance inheriting from BaseRouter.

    Raises:
        KeyError: If provider is unknown.
    """
    # Provider-specific validation
    if provider == "sharadar" and (api_key is None or str(api_key).strip() == ""):
        raise ValueError("Sharadar router requires a non-empty api_key")
    if provider == "sharadar":
        factory = _sharadar_factory
    elif provider == "yfinance":
        factory = _yfinance_factory
    else:
        raise KeyError(f"Unsupported router: {provider}") from None
    return factory(
        api_key=api_key,
        rate_limit=rate_limit,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


__all__ = ["BaseRouter", "create_router"]
