from __future__ import annotations

from typing import Protocol, TypeVar, Generic, Sequence, AsyncIterator, Callable, TYPE_CHECKING

from vertex_forager.core.config import FetchJob, ParseResult
if TYPE_CHECKING:
    from vertex_forager.writers.base import BaseWriter
    from vertex_forager.schema.mapper import SchemaMapper


T = TypeVar("T", bound=str)
T_contra = TypeVar("T_contra", bound=str, contravariant=True)


class IRouter(Protocol, Generic[T_contra]):
    """Provider-agnostic Router protocol."""

    @property
    def provider(self) -> str: ...

    async def generate_jobs(self, *, dataset: T_contra, symbols: Sequence[str] | None, **kwargs: object) -> AsyncIterator[FetchJob]: ...

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
        **kwargs: object,
    ): ...
