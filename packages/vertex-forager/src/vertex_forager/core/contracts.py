from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from vertex_forager.core.config import FramePacket, HTTPConfig
    from vertex_forager.writers.base import WriteResult


@runtime_checkable
class HttpClientProtocol(Protocol):
    """Minimal async HTTP client protocol used by HttpExecutor."""

    _http_limits: HTTPConfig

    async def run_async(self, method: str, url: str, **kwargs: Any) -> Any: ...

    async def run_sync(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any: ...


@runtime_checkable
class TracerProtocol(Protocol):
    """Minimal tracing protocol used by the pipeline for optional spans."""

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, object] | None = None,
    ) -> AbstractContextManager[object] | None: ...


class IWriter(Protocol):
    """Writer protocol for persisting normalized packets."""

    async def write(self, packet: FramePacket) -> WriteResult:
        """Persist a normalized packet.

        Args:
            packet (FramePacket): The normalized packet produced by the mapper.

        Returns:
            WriteResult: Result metadata (e.g., rows written, conflicts).
        """
        ...

    async def flush(self) -> None:
        """Flush any buffered data to the destination.

        Returns:
            None

        Notes:
            Implementations should ensure buffered frames are durably written
            and release any temporary resources associated with batching.
        """
        ...


class BaseMapper(ABC):
    """Abstract base class for packet normalization."""

    @abstractmethod
    def normalize(self, *, packet: FramePacket) -> FramePacket:
        """Normalize a packet to the target schema.

        Args:
            packet (FramePacket): Input packet with provider-specific fields/types.

        Returns:
            FramePacket: Output packet aligned to sink schema (types/columns).
        """
        ...


__all__ = [
    "BaseMapper",
    "HttpClientProtocol",
    "IWriter",
    "TracerProtocol",
]
