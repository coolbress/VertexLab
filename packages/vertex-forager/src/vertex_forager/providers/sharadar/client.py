from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import psutil

from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import RunResult
from vertex_forager.routers import create_router
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.writers import create_writer
from vertex_forager.utils import (
    jupyter_safe,
    process_symbols,
    check_memory_safety,
    create_pbar_updater,
    Spinner,
)
from tqdm import tqdm
from vertex_forager.providers.sharadar.utils import DATASET_TABLE, OptimizedBulkCalculator

logger = logging.getLogger(__name__)


class SharadarClient(BaseClient):
    """Sharadar-specific client exposing Sharadar data APIs."""

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
        self._optimizer = OptimizedBulkCalculator()
        self._metadata_cache: pl.DataFrame | None = None

    @property
    def cached_metadata(self) -> pl.DataFrame | None:
        """Return the cached full metadata if available."""
        return self._metadata_cache

    def clear_cache(self) -> None:
        """Clear the metadata cache."""
        self._metadata_cache = None

    # ----------------------------------------------------------------
    # Internal Helpers & Core Patterns
    # ----------------------------------------------------------------
    def _validate_input(
        self, symbols: list[str] | None, connect_db: str | Path | None
    ) -> None:
        """Validate input and check memory safety."""
        if symbols is None and connect_db is None:
            raise ValueError("Either tickers or connect_db must be provided")

        if connect_db is not None:
            return

        BYTES_PER_TICKER = 1 * 1024 * 1024  # 1MB
        ESTIMATED_TOTAL_TICKERS = 15_000

        num_tickers = len(symbols) if symbols else ESTIMATED_TOTAL_TICKERS
        estimated_size = num_tickers * BYTES_PER_TICKER
        available_memory = psutil.virtual_memory().available

        check_memory_safety(estimated_size, available_memory, num_tickers)

    async def _create_optimized_bulk(
        self,
        symbols: list[str],
        dataset: str,
        table_name: str,
        start_date: str | None,
        end_date: str | None,
        **kwargs: object,
    ) -> list[str]:
        """Create optimized bulk batches for the given symbols."""
        if dataset == "tickers":
            # For TICKERS dataset, manually chunk into efficient bulk batches (e.g. 100)
            # so the Router receives pre-batched strings and creates 1 job per string.
            # Using self.get_tickers() recursively here would be infinite loop if not careful.
            # But get_tickers calls _fetch_by_tickers which calls this.
            # We just need simple chunking here for metadata fetch itself.
            CHUNK_SIZE = 100
            batches = []
            for i in range(0, len(symbols), CHUNK_SIZE):
                chunk = symbols[i : i + CHUNK_SIZE]
                batches.append(",".join(chunk))
            return batches

        # 1. Fetch Metadata (Smart Batching)
        # Apply Smart Batching to ALL ticker-based datasets.
        # This ensures consistent optimization regardless of data density (daily/quarterly).
        meta_df = self._metadata_cache
        if meta_df is None:
            try:
                # Use Spinner only if we are actually fetching metadata
                # and not just using cache.
                with Spinner("Fetching metadata for Smart Batching..."):
                    # Call internal method directly to avoid jupyter_safe wrapper overhead
                    # and nested Spinners.
                    result = await self._fetch_by_tickers(
                        dataset="tickers",
                        desc="Fetching metadata",
                        table_name=DATASET_TABLE["tickers"],
                        tickers=symbols,
                        connect_db=None,
                        show_progress=False,
                    )
                    if isinstance(result, pl.DataFrame):
                        meta_df = result
            except Exception as e:
                logger.warning(
                    f"Failed to fetch metadata: {e}. Falling back to heuristic."
                )

        # 2. Optimize Batches (Bin Packing)
        # Now supports table-aware estimation (SF1 vs SEP)
        # Note: Even if estimation fails and a batch exceeds 10,000 rows, 
        # the SharadarRouter handles pagination (cursor_id) automatically as a safety net,
        # ensuring no data loss occurs.
        return self._optimizer.optimize(
            symbols, meta_df, start_date, end_date, table_name
        )

    async def _fetch_pagination(
        self,
        dataset: str,
        desc: str,
        table_name: str,
        connect_db: str | Path | None,
        *,
        show_progress: bool = True,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Pattern 1: Fetch full dataset via pagination (e.g., SP500, All Tickers)."""
        writer = create_writer(connect_db)
        # Manually enter writer context to control closing with Spinner
        await writer.__aenter__()

        try:
            router = create_router(
                "sharadar",
                api_key=self.api_key,  # type: ignore[arg-type] # Validated in __init__
                config=self._config,
                start_date=None,
                end_date=None,
            )

            with tqdm(desc=desc, unit="pages", disable=not show_progress) as pbar:
                run_result = await self._run(
                    router=router,
                    dataset=dataset,
                    symbols=None,  # Always None for pure pagination
                    writer=writer,
                    mapper=self._mapper,
                    on_progress=create_pbar_updater(pbar) if show_progress else None,
                    **kwargs,
                )
        
            self.last_run = run_result
            
            final_result = run_result
            if connect_db is None:
                final_result = writer.collect_table(table_name)

            # Cache metadata if this was a full tickers fetch (explicit get_tickers call)
            if (
                dataset == "tickers"
                and isinstance(final_result, pl.DataFrame)
            ):
                logger.debug("Caching full metadata from explicit get_tickers() call.")
                self._metadata_cache = final_result

            return final_result

        finally:
            # Ensure writer is closed properly with user feedback
            if show_progress:
                logger.info("Fetching complete. Finalizing database writes...")
                with Spinner("Finalizing database writes..."):
                    await writer.__aexit__(None, None, None)
            else:
                await writer.__aexit__(None, None, None)

    async def _fetch_by_tickers(
        self,
        dataset: str,
        desc: str,
        table_name: str,
        tickers: list[str] | None,
        connect_db: str | Path | None,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        show_progress: bool = True,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Pattern 2: Fetch specific tickers with Smart Batching (Auto)."""
        symbols = process_symbols(tickers)
        
        # Validation is only needed if we are fetching specific tickers into memory
        if symbols:
            self._validate_input(symbols, connect_db)
            # Store original ticker count for correct progress bar total
            total_tickers = len(symbols)
            symbols = await self._create_optimized_bulk(
                symbols, dataset, table_name, start_date, end_date, **kwargs
            )
        else:
            total_tickers = None

        writer = create_writer(connect_db)
        # Manually enter writer context to control closing with Spinner
        await writer.__aenter__()

        try:
            router = create_router(
                "sharadar",
                api_key=self.api_key,  # type: ignore[arg-type] # Validated in __init__
                config=self._config,
                start_date=start_date,
                end_date=end_date,
            )
            
            # Use original ticker count if available, otherwise None
            total = total_tickers

            with tqdm(
                total=total,
                unit="tickers",
                desc=desc,
                dynamic_ncols=True,
                disable=not show_progress,
            ) as pbar:
                run_result = await self._run(
                    router=router,
                    dataset=dataset,
                    symbols=symbols,
                    writer=writer,
                    mapper=self._mapper,
                    on_progress=create_pbar_updater(pbar) if show_progress else None,
                    **kwargs,
                )

            self.last_run = run_result
            
            final_result = run_result
            if connect_db is None:
                final_result = writer.collect_table(table_name)

            # Cache metadata if this was a full tickers fetch (explicit get_tickers call)
            if (
                dataset == "tickers"
                and tickers is None
                and isinstance(final_result, pl.DataFrame)
            ):
                logger.debug("Caching full metadata from explicit get_tickers() call.")
                self._metadata_cache = final_result

            return final_result

        finally:
            # Ensure writer is closed properly with user feedback
            if show_progress:
                logger.info("Fetching complete. Finalizing database writes...")
                with Spinner("Finalizing database writes..."):
                    await writer.__aexit__(None, None, None)
            else:
                await writer.__aexit__(None, None, None)

    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------
    @jupyter_safe
    async def get_tickers(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch metadata for all or specific tickers (TICKERS)."""
        # If tickers are provided, we use ticker pattern. If not, we use pagination pattern?
        # Actually, sharadar tickers endpoint supports ticker filtering.
        # So we can use _fetch_by_tickers for both cases.
        # If tickers is None, it fetches all (pagination).
        # Use Spinner to indicate preparation phase (as requested by user)
        with Spinner("Fetching ticker metadata..."):
            return await self._fetch_by_tickers(
                dataset="tickers",
                desc="Fetching tickers metadata",
                table_name=DATASET_TABLE["tickers"],
                tickers=tickers,
                connect_db=connect_db,
                show_progress=False,
                **kwargs,
            )

    # Alias for user convenience
    get_ticker_info = get_tickers

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
            desc="Fetching S&P 500 history",
            table_name=DATASET_TABLE["sp500"],
            connect_db=connect_db,
            **kwargs,
        )

    @jupyter_safe
    async def get_price_data(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch price data (SEP)."""
        return await self._fetch_by_tickers(
            dataset="price",
            desc="Fetching price data",
            table_name=DATASET_TABLE["price"],
            tickers=tickers,
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    @jupyter_safe
    async def get_fundamental_data(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        dimension: str = "MRT",
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch fundamental data (SF1)."""
        return await self._fetch_by_tickers(
            dataset="fundamental",
            desc="Fetching fundamental data",
            table_name=DATASET_TABLE["fundamental"],
            tickers=tickers,
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            dimension=dimension,
            **kwargs,
        )

    @jupyter_safe
    async def get_daily_metrics(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch daily metrics (DAILY)."""
        return await self._fetch_by_tickers(
            dataset="daily",
            desc="Fetching daily metrics",
            table_name=DATASET_TABLE["daily"],
            tickers=tickers,
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    @jupyter_safe
    async def get_corporate_actions(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch corporate actions (ACTIONS)."""
        return await self._fetch_by_tickers(
            dataset="actions",
            desc="Fetching corporate actions",
            table_name=DATASET_TABLE["actions"],
            tickers=tickers,
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    @jupyter_safe
    async def get_insider_trading(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch insider trading data (SF2)."""
        return await self._fetch_by_tickers(
            dataset="insider",
            desc="Fetching insider trading data",
            table_name=DATASET_TABLE["insider"],
            tickers=tickers,
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    @jupyter_safe
    async def get_institutional_ownership(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch institutional ownership data (SF3)."""
        return await self._fetch_by_tickers(
            dataset="institutional",
            desc="Fetching institutional ownership",
            table_name=DATASET_TABLE["institutional"],
            tickers=tickers,
            connect_db=connect_db,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )


ForagerClient = SharadarClient
