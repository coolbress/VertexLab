from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl
import duckdb

from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import RunResult
from vertex_forager.routers import create_router
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.utils import (
    jupyter_safe,
    validate_memory_usage,
    Spinner,
)
from vertex_forager.providers.sharadar.schema import DATASET_TABLE

logger = logging.getLogger(__name__)


class SharadarClient(BaseClient):
    """Sharadar-specific client exposing Sharadar data APIs."""

    BYTES_PER_TICKER_METADATA = 1024  # 1KB for metadata
    BYTES_PER_TICKER_FULL = 1 * 1024 * 1024  # 1MB for price/financials
    ESTIMATED_TOTAL_TICKERS = 15_000

    def __init__(
        self,
        *,
        api_key: str,
        rate_limit: int,
        **kwargs: object,
    ) -> None:
        """Initialize the Sharadar client.

        Args:
            api_key: Valid API key for the provider.
            rate_limit: Requests per minute (int).
            **kwargs: Additional configuration parameters for EngineConfig.
        """
        if not api_key:
            raise ValueError("Sharadar API Key is missing")

        super().__init__(
            api_key=api_key,
            rate_limit=rate_limit,
            **kwargs,
        )

        self._mapper = SchemaMapper()
        self._metadata_cache: pl.DataFrame | None = None
        
    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------
    @jupyter_safe
    async def get_ticker_info(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch metadata for all or specific tickers (TICKERS)."""
        if tickers:
            result = await self._fetch_per_ticker(
                dataset="tickers",
                symbols=tickers,
                connect_db=connect_db,
                desc="Fetching tickers metadata",
                table_name=DATASET_TABLE["tickers"],
                show_progress=False,
                total_items=len(tickers),
                **kwargs,
            )
        else:
            result = await self._fetch_pagination(
                dataset="tickers",
                connect_db=connect_db,
                desc="Fetching all tickers metadata",
                table_name=DATASET_TABLE["tickers"],
                show_progress=False,
                **kwargs,
            )
        # Auto-cache ticker metadata: in-memory when result is a DataFrame, or load from DuckDB when connected.
        if isinstance(result, pl.DataFrame):
            self._metadata_cache = result
        elif connect_db:
            try:
                table = DATASET_TABLE["tickers"]
                conn = duckdb.connect(str(connect_db))
                df_pd = conn.execute(
                    f"SELECT ticker, firstpricedate, lastpricedate FROM {table}"
                ).fetch_df()
                conn.close()
                self._metadata_cache = pl.from_pandas(df_pd)
            except Exception:
                pass
        return result

    @jupyter_safe
    async def get_sp500_history(
        self,
        *,
        connect_db: str | Path | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch S&P 500 component history."""
        # SP500 is typically full history fetch, best suited for pagination
        return await self._fetch_pagination(
            dataset="sp500",
            connect_db=connect_db,
            desc="Fetching S&P 500 history",
            table_name=DATASET_TABLE["sp500"],
            **kwargs,
        )

    @jupyter_safe
    async def get_price_data(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Get price data for specified tickers.

        This method delegates to `fetch_per_ticker` to retrieve price data.

        Args:
            tickers: List of ticker symbols to fetch data for.
            connect_db: Path to DuckDB database file for storing results.
            start_date: Start date for data fetching (YYYY-MM-DD).
            end_date: End date for data fetching (YYYY-MM-DD).
            **kwargs: Additional arguments passed to the fetcher.

        Returns:
            polars.DataFrame | RunResult: DataFrame if fetching in-memory,
            or RunResult object if storing to database.

        Raises:
            ValueError: If neither tickers nor connect_db is provided.
            httpx.RequestError: If a network error occurs.
            httpx.HTTPStatusError: If the API returns a non-success status code.
        """
        return await self._fetch_per_ticker(
            dataset="price",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching price data",
            table_name=DATASET_TABLE["price"],
            start_date=start_date,
            end_date=end_date,
            total_items=len(tickers),
            **kwargs,
        )

    @jupyter_safe
    async def get_fundamental_data(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        dimension: str = "MRT",
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch fundamental data (SF1)."""
        return await self._fetch_per_ticker(
            dataset="fundamental",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching fundamental data",
            table_name=DATASET_TABLE["fundamental"],
            start_date=start_date,
            end_date=end_date,
            total_items=len(tickers),
            dimension=dimension,
            **kwargs,
        )

    @jupyter_safe
    async def get_daily_metrics(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch daily metrics (DAILY)."""
        return await self._fetch_per_ticker(
            dataset="daily",
            symbols=tickers,
            desc="Fetching daily metrics",
            table_name=DATASET_TABLE["daily"],
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )

    @jupyter_safe
    async def get_corporate_actions(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch corporate actions (ACTIONS)."""
        return await self._fetch_per_ticker(
            dataset="actions",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching corporate actions",
            table_name=DATASET_TABLE["actions"],
            start_date=start_date,
            end_date=end_date,
            total_items=len(tickers),
            **kwargs,
        )

    @jupyter_safe
    async def get_insider_trading(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch insider trading data (SF2)."""
        return await self._fetch_per_ticker(
            dataset="insider",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching insider trading data",
            table_name=DATASET_TABLE["insider"],
            start_date=start_date,
            end_date=end_date,
            total_items=len(tickers),
            **kwargs,
        )

    @jupyter_safe
    async def get_institutional_ownership(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch institutional ownership data (SF3)."""
        return await self._fetch_per_ticker(
            dataset="institutional",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching institutional ownership",
            table_name=DATASET_TABLE["institutional"],
            start_date=start_date,
            end_date=end_date,
            total_items=len(tickers),
            **kwargs,
        )

    # ----------------------------------------------------------------
    # Internal Data Fetchers 
    # ----------------------------------------------------------------
    async def _fetch_per_ticker(
        self,
        *,
        dataset: str,
        symbols: list[str] | None,
        connect_db: str | Path | None,
        desc: str,
        table_name: str,
        show_progress: bool = True,
        total_items: int | None = None,
        unit: str = "tickers",
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch data for specific tickers using per-ticker batching.
        
        This method implements the per-ticker fetching pattern using BaseClient's
        common infrastructure while maintaining Sharadar-specific logic for
        metadata caching and memory validation.
        
        Sharadar-specific characteristics:
        - Metadata caching for smart batching optimization
        - Memory validation for large dataset safety
        - Priority queue-based request scheduling
        - Automatic metadata prefetch for optimal performance
        
        Args:
            dataset: Dataset name (e.g., "price", "fundamental")
            symbols: List of symbols to fetch, or None for all symbols
            connect_db: Database connection string/path, or None for in-memory
            desc: Progress bar description
            table_name: Table name for result collection
            show_progress: Whether to show progress indicators
            total_items: Total number of items (for progress bar)
            unit: Unit label for progress bar (default: "tickers")
            start_date: Start date for data fetch
            end_date: End date for data fetch
            **kwargs: Additional provider-specific arguments
            
        Returns:
            pl.DataFrame for in-memory mode, RunResult for database mode
        """
        bytes_per_item = (
            self.BYTES_PER_TICKER_METADATA if dataset == "tickers" else self.BYTES_PER_TICKER_FULL
        )
        validate_memory_usage(
            symbols=symbols,
            connect_db=connect_db,
            bytes_per_item=bytes_per_item,
        )

        # Sharadar-specific: Metadata caching for smart batching
        if self._metadata_cache is None and dataset != "tickers":
            logger.info("Metadata cache miss. Fetching ticker metadata first...")
            try:
                with Spinner("Prefetching metadata for smart batching..."):
                    meta_result = await self.get_ticker_info.__wrapped__(self, tickers=None, connect_db=None)
                    if isinstance(meta_result, pl.DataFrame):
                        self._metadata_cache = meta_result
                        logger.info(f"Metadata cached: {len(self._metadata_cache)} tickers")
            except Exception as e:
                logger.warning(f"Failed to prefetch metadata: {e}. Smart batching will be disabled.")

        # Sharadar-specific: Determine total items for progress
        if symbols:
            total_items = len(symbols)
        elif dataset == "tickers":
            total_items = self.ESTIMATED_TOTAL_TICKERS
        else:
            total_items = None

        # Use BaseClient's common infrastructure
        pbar, pbar_updater = self.create_progress_tracker(
            total_items=total_items,
            unit=unit,
            desc=desc,
            show_progress=show_progress,
        )
        
        result_obj: pl.DataFrame | RunResult
        async with self.managed_writer(connect_db, show_progress=show_progress) as writer:
            try:
                router = create_router(
                    "sharadar",
                    api_key=self.api_key,  # type: ignore[arg-type]
                    config=self._config,
                    start_date=start_date,
                    end_date=end_date,
                    ticker_metadata=self._metadata_cache,
                    **kwargs,
                )
                
                await self.run_pipeline(
                    router=router,
                    dataset=dataset,
                    symbols=symbols,
                    writer=writer,
                    mapper=self._mapper,
                    on_progress=pbar_updater,
                    **kwargs,
                )
                
                result_obj = await self.collect_results(
                    writer=writer,
                    table_name=table_name,
                    connect_db=connect_db,
                )
                
                if dataset == "tickers" and isinstance(result_obj, pl.DataFrame):
                    logger.debug("Caching full metadata from explicit get_ticker_info() call.")
                    self._metadata_cache = result_obj
            finally:
                if pbar is not None:
                    pbar.close()
        return result_obj

    async def _fetch_pagination(
        self,
        *,
        dataset: str,
        connect_db: str | Path | None,
        desc: str,
        table_name: str,
        show_progress: bool = True,
        unit: str = "pages",
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch full dataset via pagination (e.g., SP500, All Tickers).
        
        This method implements the pagination fetching pattern using BaseClient's
        common infrastructure while maintaining Sharadar-specific logic for
        large dataset handling.
        
        Sharadar-specific characteristics:
        - Memory validation for large dataset safety
        - Pagination-based request scheduling
        - Optimized for bulk data fetching
        
        Args:
            dataset: Dataset name (e.g., "sp500", "tickers")
            connect_db: Database connection string/path, or None for in-memory
            desc: Progress bar description
            table_name: Table name for result collection
            show_progress: Whether to show progress indicators
            unit: Unit label for progress bar (default: "pages")
            **kwargs: Additional provider-specific arguments
            
        Returns:
            pl.DataFrame for in-memory mode, RunResult for database mode
        """

        use_pbar = show_progress and dataset not in ("sp500", "tickers")
        if use_pbar:
            pbar, pbar_updater = self.create_progress_tracker(
                total_items=None,
                unit=unit,
                desc=desc,
                show_progress=True,
            )
        else:
            pbar, pbar_updater = None, None
        
        result_obj: pl.DataFrame | RunResult
        async with self.managed_writer(connect_db, show_progress=show_progress) as writer:
            try:
                router = create_router(
                    "sharadar",
                    api_key=self.api_key,  # type: ignore[arg-type]
                    config=self._config,
                    ticker_metadata=self._metadata_cache,
                    **kwargs,
                )
                
                with Spinner(desc, persist=True):
                    await self.run_pipeline(
                        router=router,
                        dataset=dataset,
                        symbols=None,
                        writer=writer,
                        mapper=self._mapper,
                        on_progress=pbar_updater,
                        **kwargs,
                    )
                
                result_obj = await self.collect_results(
                    writer=writer,
                    table_name=table_name,
                    connect_db=connect_db,
                )
            finally:
                if pbar is not None:
                    pbar.close()
        return result_obj
