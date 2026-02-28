"""Client implementations for different data providers."""

from __future__ import annotations

import os

import logging
from vertex_forager.clients.base import BaseClient
from vertex_forager.core.registries import (
    clients as client_registry,
    ClientRegistration,
)


def _register_sharadar() -> None:
    from vertex_forager.providers.sharadar.client import SharadarClient
    from typing import Any
    def _sharadar_factory(*, api_key: str | None = None, rate_limit: int, **kwargs: Any) -> BaseClient:
        return SharadarClient(api_key=api_key or "", rate_limit=rate_limit, **kwargs)

    # Register known providers
    client_registry.register(
        "sharadar",
        ClientRegistration(
            env_api_key="SHARADAR_API_KEY",
            factory=_sharadar_factory,
        ),
    )


def _register_yfinance() -> None:
    from vertex_forager.providers.yfinance.client import YFinanceClient
    from typing import Any
    def _yfinance_factory(*, api_key: str | None = None, rate_limit: int, **kwargs: Any) -> BaseClient:
        return YFinanceClient(api_key=api_key or "", rate_limit=rate_limit, **kwargs)

    client_registry.register(
        "yfinance",
        ClientRegistration(
            env_api_key=None,
            factory=_yfinance_factory,
        ),
    )


def create_client(
    *,
    provider: str,
    api_key: str | None = None,
    rate_limit: int | None = None,
    **kwargs: object,
) -> BaseClient:
    """
    Create and configure a client instance for the specified provider.

    Args:
        provider: The provider identifier (e.g., "sharadar").
        api_key: API key. If not provided, will look up provider-specific env var.
        rate_limit: Rate limit in requests per minute.
        **kwargs: Additional configuration passed to the client.

    Returns:
        Configured client instance inheriting from BaseClient.

    Raises:
        ValueError: If API key is missing.
        KeyError: If provider is unknown.
    """

    try:
        registration = client_registry.get(provider)
    except KeyError:
        # Lazy loading for known internal providers to avoid circular imports
        if provider == "sharadar":
            _register_sharadar()
            registration = client_registry.get(provider)
        elif provider == "yfinance":
            _register_yfinance()
            registration = client_registry.get(provider)
        else:
            raise KeyError(f"Unsupported client: {provider}") from None

    resolved_key = api_key
    if not resolved_key and registration.env_api_key:
        resolved_key = os.getenv(registration.env_api_key)

    if not resolved_key and registration.env_api_key:
        raise ValueError(
            f"Missing api_key (set api_key or {registration.env_api_key} in environment/.env)"
        )

    if provider == "yfinance":
        if api_key is not None or kwargs.get("api_key") is not None or resolved_key is not None:
            logging.getLogger(__name__).warning("Provided API key will be ignored for yfinance; continuing with api_key=None")
        return registration.factory(api_key=None, rate_limit=rate_limit if rate_limit is not None else 60, **kwargs)
    if rate_limit is None:
        raise ValueError(f"Missing rate_limit for provider '{provider}'")
    return registration.factory(api_key=resolved_key, rate_limit=rate_limit, **kwargs)


__all__ = ["BaseClient", "create_client"]
