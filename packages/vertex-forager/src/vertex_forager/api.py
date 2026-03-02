"""Public API for vertex-forager.
 
This module is the primary import surface for users of the library.
 
Usage:
    from vertex_forager.api import create_client, create_router
    
    # Create client and router for a provider
    client = create_client(provider="sharadar", api_key="...", rate_limit=120)
    router = create_router(provider="sharadar", api_key="...", rate_limit=120)
    
    # Run a pipeline via client (preferred)
    # await client.run_pipeline(router=router, dataset="price", symbols=["AAPL"], writer=..., mapper=...)
 
Exports:
    BaseClient, BaseRouter, SharadarClient, YFinanceClient, create_client, create_router
"""

from __future__ import annotations

from vertex_forager.clients.base import BaseClient
from vertex_forager.clients import create_client
from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.routers.base import BaseRouter
from vertex_forager.routers import create_router


__all__ = [
    "BaseClient",
    "BaseRouter",
    "SharadarClient",
    "YFinanceClient",
    "create_client",
    "create_router",
]
