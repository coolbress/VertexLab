"""
Core module for Vertex Forager.

This module contains the fundamental components of the scraping engine:
- `pipeline`: Main orchestration logic (`VertexForager`).
- `config`: Configuration data structures (`EngineConfig`, `FetchJob`, etc.).
- `controller`: Flow control (Concurrency & Rate Limiting).
- `http`: Low-level HTTP execution.
- `retry`: Retry strategies.
"""
from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from . import retry

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
