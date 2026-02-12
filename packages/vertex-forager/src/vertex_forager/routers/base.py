from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence

from vertex_forager.core.config import FetchJob, ParseResult


class BaseRouter(ABC):
    """
    Vendor-agnostic base router abstraction for the Vertex Forager pipeline.

    The Router acts as the protocol adapter between the generic pipeline engine and 
    specific vendor APIs. It encapsulates all knowledge about URL construction, 
    request parameters, and response parsing logic.

    Key Responsibilities:
    1. **Job Generation (`generate_jobs`)**: Translates high-level data requests (dataset, symbols)
       into concrete `FetchJob` objects containing fully formed HTTP request specifications.
       - Handles pagination logic (generating multiple jobs if needed).
       - Applies provider-specific query parameters.
    2. **Response Parsing (`parse`)**: Converts raw HTTP response bytes into structured 
       `FramePacket`s (Polars DataFrames) normalized for the pipeline.
       - Handles CSV/JSON parsing.
       - Validates response schemas.
       - Maps vendor-specific field names to pipeline standards.

    Design Principles:
    - **Statelessness**: Routers should be primarily stateless, processing inputs to outputs 
      without maintaining complex internal state about the pipeline progress.
    - **Isolation**: Each Router implementation (e.g., `SharadarRouter`) contains all 
      vendor-specific logic, keeping the core pipeline engine clean and generic.
    """

    @property
    @abstractmethod
    def provider(self) -> str:
        """Unique identifier for the data provider (e.g., 'sharadar')."""
        raise NotImplementedError

    @abstractmethod
    async def generate_jobs(
        self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        """Generate fetch jobs for the pipeline to execute."""

    @abstractmethod
    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        """Parse a raw HTTP response into data packets."""

