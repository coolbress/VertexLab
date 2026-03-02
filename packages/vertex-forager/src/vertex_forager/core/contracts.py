from __future__ import annotations

from typing import Protocol, TypeVar, Generic, Sequence, AsyncIterator, Callable, TYPE_CHECKING, Union
from vertex_forager.core.types import JSONValue
from vertex_forager.core.types import SharadarDataset, YFinanceDataset

from vertex_forager.core.config import FetchJob, ParseResult, RunResult
if TYPE_CHECKING:
    from vertex_forager.writers.base import BaseWriter, WriteResult
    from vertex_forager.schema.mapper import SchemaMapper
    from vertex_forager.core.config import FramePacket


T = TypeVar("T", bound=Union[SharadarDataset, YFinanceDataset, str])
T_contra = TypeVar("T_contra", bound=Union[SharadarDataset, YFinanceDataset, str], contravariant=True)


class IRouter(Protocol, Generic[T_contra]):
    """Provider-agnostic Router protocol."""

    @property
    def provider(self) -> str: ...

    def generate_jobs(self, *, dataset: T_contra, symbols: Sequence[str] | None, **kwargs: object) -> AsyncIterator[FetchJob]: ...

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult: ...


class IClient(Protocol, Generic[T]):
    """Client protocol for running the pipeline."""

    async def run_pipeline(
        self,
        *,
        router: IRouter[T],
        dataset: T,
        symbols: list[str] | None,
        writer: "BaseWriter",
        mapper: "SchemaMapper",
        on_progress: Callable[..., None] | None = None,
        **kwargs: JSONValue,
    ) -> RunResult: ...


class IWriter(Protocol):
    """Writer protocol for persisting normalized packets."""

    async def write(self, packet: "FramePacket") -> "WriteResult": ...
    async def flush(self) -> None: ...


class IMapper(Protocol):
    """Schema mapper protocol for normalizing packets."""

    def normalize(self, *, packet: "FramePacket") -> "FramePacket": ...
