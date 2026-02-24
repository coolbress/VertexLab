"""Router implementations for different data sources."""

from __future__ import annotations

from typing import Any

from vertex_forager.core.config import EngineConfig
from vertex_forager.routers.base import BaseRouter
# from vertex_forager.providers.sharadar.router import SharadarRouter
# from vertex_forager.providers.yfinance.router import YFinanceRouter
from vertex_forager.core.registries import (
    routers as router_registry,
    RouterRegistration,
)

def _sharadar_factory(**kwargs: Any) -> BaseRouter:
    from vertex_forager.providers.sharadar.router import SharadarRouter
    return SharadarRouter(**kwargs)

def _yfinance_factory(**kwargs: Any) -> BaseRouter:
    from vertex_forager.providers.yfinance.router import YFinanceRouter
    return YFinanceRouter(**kwargs)

# Register known providers
router_registry.register(
    "sharadar",
    RouterRegistration(factory=_sharadar_factory),
)
router_registry.register(
    "yfinance",
    RouterRegistration(factory=_yfinance_factory),
)


def create_router(
    provider: str,
    *,
    api_key: str,
    config: EngineConfig,
    start_date: str | None = None,
    end_date: str | None = None,
    **kwargs: Any,
) -> BaseRouter:
    """
    Create and configure a router instance for the specified provider.

    Args:
        provider: The provider identifier (e.g., "sharadar").
        api_key: API key.
        config: Engine configuration containing rate limits.
        start_date: Optional start date filter.
        end_date: Optional end date filter.
        **kwargs: Additional provider-specific configuration.

    Returns:
        Configured router instance inheriting from BaseRouter.

    Raises:
        KeyError: If provider is unknown.
    """
    registration = router_registry.get(provider)

    # We pass explicit arguments that match the RouterFactory protocol/signature
    # assuming most routers will need these standard parameters.
    return registration.factory(
        api_key=api_key,
        rate_limit=config.requests_per_minute,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


__all__ = ["BaseRouter", "create_router"]
