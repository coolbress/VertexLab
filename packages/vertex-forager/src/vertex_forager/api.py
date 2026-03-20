"""Public API for vertex-forager.

This module is the primary import surface for end users. Prefer composing
clients and routers via factory helpers to keep code decoupled from concrete
implementations (DIP-friendly).

Usage:
    from vertex_forager.api import create_client, create_router

    client = create_client(provider=\"sharadar\", api_key=\"...\", rate_limit=120)
    router = create_router(provider="sharadar", api_key="...", config=client.config)

    # Run a pipeline through the client (recommended)
    # await client.run_pipeline(router=router, dataset=\"price\", symbols=[\"AAPL\"], writer=..., mapper=...)

Notes:
    - Concrete classes (e.g., SharadarClient, YFinanceClient) are available, but
      typical usage does not require importing them directly; prefer factory helpers.
"""

from __future__ import annotations

from vertex_forager.clients import create_client
from vertex_forager.clients.base import BaseClient
from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.routers import create_router
from vertex_forager.routers.base import BaseRouter

__all__ = [
    "BaseClient",
    "BaseRouter",
    "SharadarClient",
    "YFinanceClient",
    "create_client",
    "create_router",
]
