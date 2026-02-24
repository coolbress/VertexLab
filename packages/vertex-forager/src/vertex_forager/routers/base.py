from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from datetime import datetime

import polars as pl

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
        """Unique identifier for the data provider.
        
        Returns:
            str: Provider identifier (e.g., 'sharadar', 'yfinance').
        """
        raise NotImplementedError

    @abstractmethod
    async def generate_jobs(
        self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        """Generate provider-specific HTTP fetch jobs.
        
        Converts high-level requests (dataset, symbols, date filters) into concrete
        HTTP request specifications. Provider implementations own batching, URL
        construction, auth, and pagination context.
        
        Args:
            dataset: Dataset type (e.g., 'price', 'financials', 'actions').
            symbols: List of ticker symbols, or None for market-wide data.
            **kwargs: Provider-specific options (e.g., dimension, bulk_size).
            
        Yields:
            FetchJob: HTTP request spec and context for the executor.
            
        Raises:
            NotImplementedError: Must be implemented by provider routers.
        
        Responsibilities:
            - Symbol handling: single/multiple/None depending on dataset
            - Batching: choose batch size under API/URL limits
            - URL/params: build endpoints and query parameters
            - Date filters: apply start/end via _parse_date_range when needed
            - Pagination: include cursor/keys in context if supported
            - Auth: attach provider tokens/headers
        """

    @abstractmethod
    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        """Normalize provider response into FramePacket(s).
        
        Decodes raw bytes, converts to Polars, performs provider-specific structural
        normalization in this method (no separate normalize_frame), injects provider
        metadata, and prepares follow-up pagination jobs when applicable.
        
        Args:
            job: The fetch job that produced this payload.
            payload: Raw HTTP response bytes.
            
        Returns:
            ParseResult: Normalized packets and optional next jobs.
            
        Raises:
            NotImplementedError: Must be implemented by provider routers.
        
        Responsibilities:
            - Format handling: JSON/CSV/binary/pickle
            - Error handling: detect API errors and malformed payloads
            - Data extraction: parse nested provider-specific structures
            - DataFrame creation: build Polars frames
            - Column normalization: use _normalize_columns where applicable
            - Metadata injection: use _add_provider_metadata
            - Empty handling: use _check_empty_response
            - Pagination: derive next jobs from response metadata/context
        
        Notes:
            - Structure transformation happens here; strict type casting is delegated to SchemaMapper.
            - Use _parse_date_range for date derivations when needed.
        """

    # -----------------------------------------
    # Common Methods
    # -----------------------------------------
    
    def _add_provider_metadata(self, *, frame: pl.DataFrame, observed_at: datetime) -> pl.DataFrame:
        """Add provider metadata columns to DataFrame.
        
        This is a common operation across all routers to inject standard metadata
        like provider name and fetch timestamp.
        
        Args:
            frame: Input DataFrame to add metadata to.
            observed_at: Timestamp when the data was fetched.
            
        Returns:
            pl.DataFrame: DataFrame with added provider and fetched_at columns.
        """
        return frame.with_columns(
            [
                pl.lit(self.provider).alias("provider"),
                pl.lit(observed_at).alias("fetched_at"),
            ]
        )

    def _check_empty_response(self, *, payload: bytes | None = None, frame: pl.DataFrame | None = None) -> ParseResult | None:
        """Unified empty response handler for both payload and DataFrame.
        
        Args:
            payload: Raw response payload to check.
            frame: DataFrame to check.
            
        Returns:
            ParseResult: Empty result if either payload or frame is empty.
            None: If both are valid/non-empty.
        """
        if payload is not None and not payload:
            return ParseResult(packets=[], next_jobs=[])
        if frame is not None and frame.is_empty():
            return ParseResult(packets=[], next_jobs=[])
        return None

    def _parse_date_range(self, start_date: str | None, end_date: str | None) -> tuple[datetime, datetime] | None:
        """Parse and validate date range strings.
        
        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            
        Returns:
            tuple: (start_datetime, end_datetime) if valid.
            None: if start_date is None or invalid.
        """
        if not start_date:
            return None
            
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.now()
            if end_date:
                end = datetime.strptime(end_date, "%Y-%m-%d")
            return start, end
        except (ValueError, TypeError):
            return None

    def _normalize_columns(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Standardize column names to lowercase snake_case.
        
        Args:
            frame: Input DataFrame.
            
        Returns:
            pl.DataFrame: DataFrame with standardized column names.
        """
        new_columns = [c.lower().strip().replace(" ", "_") for c in frame.columns]
        return frame.rename(dict(zip(frame.columns, new_columns)))
