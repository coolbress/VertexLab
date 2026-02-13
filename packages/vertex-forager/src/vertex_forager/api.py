"""Public API for vertex-forager.

This module is intended to be the primary import surface for users.
"""

from __future__ import annotations

from vertex_forager.clients.base import BaseClient
from vertex_forager.clients import create_client
from vertex_forager.providers.sharadar.client import ForagerClient, SharadarClient
from vertex_forager.routers.base import BaseRouter
from vertex_forager.routers import create_router


__all__ = [
    "BaseClient",
    "BaseRouter",
    "SharadarClient",
    "ForagerClient",
    "create_client",
    "create_router",
]
