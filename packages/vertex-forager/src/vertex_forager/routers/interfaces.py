from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence

from vertex_forager.core.config import FetchJob, ParseResult


class BaseRouter(ABC):
    """Protocol adapter between the pipeline engine and provider APIs.
    
    Responsibilities:
        - generate_jobs: Build provider-specific FetchJob instances with batching/pagination.
        - parse: Normalize raw response bytes into structured packets and next jobs.
    """

    flexible_schema: bool = False

    @property
    @abstractmethod
    def provider(self) -> str:
        """Provider identifier (e.g., 'sharadar', 'yfinance')."""
        raise NotImplementedError

    @abstractmethod
    async def generate_jobs(
        self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        """Generate provider-specific HTTP fetch jobs."""

    @abstractmethod
    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        """Normalize provider response into FramePacket(s) and derive next jobs."""
