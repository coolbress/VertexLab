"""
Core module for Vertex Forager.

This module contains the fundamental components of the scraping engine:
- `pipeline`: Main orchestration logic (`VertexForager`).
- `config`: Configuration data structures (`EngineConfig`, `FetchJob`, etc.).
- `controller`: Flow control (Concurrency & Rate Limiting).
- `http`: Low-level HTTP execution.
- `retry`: Retry strategies.
"""

from typing import TYPE_CHECKING, Any
from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from . import retry

if TYPE_CHECKING:
    from vertex_forager.core.pipeline import VertexForager

def __getattr__(name: str) -> Any:
    """Lazy-import module attributes.
    
    Args:
        name: Attribute name to resolve.
    
    Returns:
        The VertexForager class when name == "VertexForager".
    
    Raises:
        AttributeError: If the requested attribute is not available.
    """
    if name == "VertexForager":
        from vertex_forager.core.pipeline import VertexForager
        return VertexForager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "VertexForager",
    "EngineConfig",
    "FetchJob",
    "FramePacket",
    "RunResult",
    "FlowController",
    "HttpExecutor",
    "retry",
]
