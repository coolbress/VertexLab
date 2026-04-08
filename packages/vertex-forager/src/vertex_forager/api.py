"""Public API for vertex-forager.

This module is the primary import surface for end users.

Usage:
    from vertex_forager.api import StateManager, create_client

    client = create_client(provider=\"sharadar\", api_key=\"...\", rate_limit=120)
    state = StateManager()

Notes:
    - Prefer `create_client(...)` and `StateManager()` as the stable public entry points.
    - Lower-level routers, base abstractions, and concrete provider classes remain available
      in their implementation modules when you explicitly need them.
"""

from __future__ import annotations

from vertex_forager.clients import create_client
from vertex_forager.core.config import ProgressSnapshot, RunResult
from vertex_forager.exceptions import (
    CheckpointNotFoundError,
    DataQualityError,
    FetchError,
    InputError,
    ValidationError,
    VertexForagerError,
    WriterError,
)
from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.state import DLQEntry, ReplayResult, RunRecord, StateManager

__all__ = [
    "CheckpointNotFoundError",
    "DLQEntry",
    "DataQualityError",
    "FetchError",
    "InputError",
    "ProgressSnapshot",
    "ReplayResult",
    "RunRecord",
    "RunResult",
    "SharadarClient",
    "StateManager",
    "ValidationError",
    "VertexForagerError",
    "WriterError",
    "YFinanceClient",
    "create_client",
]
