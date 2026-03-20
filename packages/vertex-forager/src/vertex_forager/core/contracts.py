from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

from vertex_forager.core.types import JSONValue, SharadarDataset, YFinanceDataset

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Sequence
    from contextlib import AbstractContextManager

    from vertex_forager.core.config import FetchJob, FramePacket, ParseResult, RunResult
    from vertex_forager.writers.base import WriteResult


T = TypeVar("T", bound=SharadarDataset | YFinanceDataset | str)
T_contra = TypeVar("T_contra", bound=SharadarDataset | YFinanceDataset | str, contravariant=True)

@runtime_checkable
class HttpClientProtocol(Protocol):
    """Minimal async HTTP client protocol used by HttpExecutor."""

    async def run_async(self, method: str, url: str, **kwargs: Any) -> Any:
        ...

    async def run_sync(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        ...


@runtime_checkable
class TracerProtocol(Protocol):
    """Minimal tracing protocol used by the pipeline for optional spans."""

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, object] | None = None,
    ) -> AbstractContextManager[object] | None:
        ...


class IRouter(Protocol, Generic[T_contra]):
    """Provider-agnostic Router protocol.

    Defines the interface expected from all router implementations that
    adapt provider APIs into the Vertex Forager pipeline.
    """

    @property
    def provider(self) -> str:
        """Provider identifier (e.g., 'sharadar', 'yfinance')."""
        ...

    def generate_jobs(
        self, *, dataset: T_contra, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        """Generate provider-specific fetch jobs.

        Args:
            dataset: Target dataset name for the provider.
            symbols: Optional sequence of target symbols; None for provider-wide fetch.
            **kwargs: Provider-specific options (e.g., dimension, date range).

        Returns:
            AsyncIterator[FetchJob]: Stream of constructed jobs.
        """
        ...

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        """Normalize provider response into packets and follow-up jobs.

        Args:
            job: Fetch job that produced the payload.
            payload: Raw response bytes.

        Returns:
            ParseResult: Extracted packets and any subsequent jobs (e.g., pagination).
        """
        ...


class IClient(Protocol, Generic[T]):
    """Client protocol for running the pipeline.

    Specifies the minimal interface the pipeline expects from clients.
    """

    async def run_pipeline(
        self,
        *,
        router: IRouter[T],
        dataset: T,
        symbols: list[str] | None,
        writer: IWriter,
        mapper: IMapper,
        on_progress: Callable[..., None] | None = None,
        **kwargs: JSONValue,
    ) -> RunResult:
        """Execute the VertexForager pipeline.

        Args:
            router: Router responsible for job generation and parsing.
            dataset: Dataset identifier for the provider.
            symbols: List of symbols, or None for provider-wide fetch.
            writer: Destination writer for normalized frames.
            mapper: Schema mapper used to enforce target types/columns.
            on_progress: Optional callback invoked per completed job.
            **kwargs: Additional pipeline options (JSONValue-safe).

        Returns:
            RunResult: Summary metrics and error collection from the run.
        """
        ...


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


class IMapper(Protocol):
    """Schema mapper protocol for normalizing packets."""

    def normalize(self, *, packet: FramePacket) -> FramePacket:
        """Normalize a packet to the target schema.

        Args:
            packet (FramePacket): Input packet with provider-specific fields/types.

        Returns:
            FramePacket: Output packet aligned to sink schema (types/columns).
        """
        ...
