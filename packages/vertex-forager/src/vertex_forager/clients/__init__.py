"""Client implementations for different data providers."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from vertex_forager.clients.base import BaseClient
from vertex_forager.core.registries import clients as client_registry, ClientRegistration

if TYPE_CHECKING:
    from vertex_forager.providers.sharadar.client import SharadarClient


def _register_sharadar() -> None:
    from vertex_forager.providers.sharadar.client import SharadarClient
    # Register known providers
    client_registry.register(
        "sharadar",
        ClientRegistration(
            env_api_key="SHARADAR_API_KEY",
            factory=SharadarClient,
        ),
    )


def create_client(
    *,
    provider: str,
    api_key: str | None = None,
    rate_limit: int,
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
        NotImplementedError: If provider is unknown.
    """
    
    try:
        registration = client_registry.get(provider)
    except NotImplementedError:
        # Lazy loading for known internal providers to avoid circular imports
        if provider == "sharadar":
            _register_sharadar()
            registration = client_registry.get(provider)
        else:
            raise

    resolved_key = api_key or os.getenv(registration.env_api_key)
    if not resolved_key:
        raise ValueError(
            f"Missing api_key (set api_key or {registration.env_api_key} in environment/.env)"
        )

    return registration.factory(
        api_key=resolved_key, 
        rate_limit=rate_limit, 
        **kwargs
    )


__all__ = ["BaseClient", "create_client"]
