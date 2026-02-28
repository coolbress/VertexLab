from __future__ import annotations

from typing import Protocol, Generic, TypeVar, Sequence, AsyncIterator, Callable, TYPE_CHECKING
from datetime import datetime
import polars as pl

from vertex_forager.core.config import FetchJob, ParseResult
if TYPE_CHECKING:
    from vertex_forager.writers.base import BaseWriter
    from vertex_forager.schema.mapper import SchemaMapper

T = TypeVar("T")


class IRouter(Protocol, Generic[T]):
    """Provider-agnostic Router protocol."""

    @property
    def provider(self) -> str: ...

    async def generate_jobs(self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object) -> AsyncIterator[FetchJob[T]]: ...

    def parse(self, *, job: FetchJob[T], payload: bytes) -> ParseResult: ...

    # Common helpers (optional)
    def _add_provider_metadata(self, *, frame: pl.DataFrame, observed_at: datetime) -> pl.DataFrame: ...
    def _check_empty_response(self, *, payload: bytes | None = None, frame: pl.DataFrame | None = None) -> ParseResult | None: ...


class IClient(Protocol):
    """Client protocol for running the pipeline."""

    async def run_pipeline(
        self,
        *,
        router: IRouter[T],
        dataset: str,
        symbols: list[str] | None,
        writer: "BaseWriter",
        mapper: "SchemaMapper",
        on_progress: Callable[..., None] | None = None,
        **kwargs: object,
    ): ...
