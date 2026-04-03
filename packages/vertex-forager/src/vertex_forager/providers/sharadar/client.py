from __future__ import annotations

from dataclasses import dataclass, field
import logging
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, cast

import duckdb
import polars as pl

from vertex_forager.clients.base import BaseClient
from vertex_forager.constants import PAGES_UNIT, RESERVED_PIPELINE_KEYS, TICKERS_UNIT
from vertex_forager.core.types import JSONValue, SharadarDataset
from vertex_forager.exceptions import InputError
from vertex_forager.providers.sharadar.constants import (
    BYTES_PER_TICKER_FULL as SH_BYTES_PER_TICKER_FULL,
)
from vertex_forager.providers.sharadar.constants import (
    BYTES_PER_TICKER_METADATA as SH_BYTES_PER_TICKER_METADATA,
)
from vertex_forager.providers.sharadar.constants import (
    ESTIMATED_TOTAL_TICKERS as SH_ESTIMATED_TOTAL_TICKERS,
)
from vertex_forager.providers.sharadar.schema import DATASET_TABLE
from vertex_forager.routers import create_router
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.utils import run_sync_compat, validate_tickers

if TYPE_CHECKING:
    from collections.abc import Callable

    from vertex_forager.core.config import (
        AdaptiveThrottleConfig,
        HTTPConfig,
        ProgressSnapshot,
        RetryConfig,
        RunResult,
        SchedulerConfig,
        StorageConfig,
    )

logger = logging.getLogger(__name__)

TICKER_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
META_REQUIRED_COLUMNS = ("ticker", "firstpricedate", "lastpricedate")


@dataclass(slots=True)
class FetchConfig:
    """Sharadar data fetch configuration.

    Attributes:
        dataset (str): Target dataset name (e.g., "price", "sp500").
        symbols (list[str] | None): List of tickers to request; None for paginated datasets.
        connect_db (str | Path | None): DuckDB file path or connection string; None for in-memory.
        desc (str): Description text for progress display.
        table_name (str): Destination table name per schema mapper.
        progress (bool): Whether to show built-in progress output for this request.
        total_items (int | None): Expected item count (bars/pages/tickers); None if unknown.
        unit (str): Unit label for progress (e.g., "tickers", "pages").
        start_date (str | None): Start date (YYYY-MM-DD) for range datasets.
        end_date (str | None): End date (YYYY-MM-DD) for range datasets.
        on_progress (Callable[[ProgressSnapshot], None] | None): Optional external progress sink.
        extra (dict[str, Any]): Extra options passed through to router/client.
    """

    dataset: SharadarDataset
    symbols: list[str] | None
    connect_db: str | Path | None
    desc: str
    table_name: str
    progress: bool = False
    total_items: int | None = None
    unit: str = TICKERS_UNIT
    start_date: str | None = None
    end_date: str | None = None
    on_progress: Callable[[ProgressSnapshot], None] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class SharadarClient(BaseClient[SharadarDataset]):
    """Client for Sharadar (Nasdaq Data Link) datasets.

    This client integrates Sharadar with the VertexForager pipeline to provide
    consistent rate limiting, logging, and error handling across datasets.

    Attributes:
        Authentication: API key required; passed via query parameter.
        Rate Limiting: Configurable requests per minute through `rate_limit`.
        Dataset Coverage: Supports `price`, `daily`, `fundamental` (SF1),
            `actions`, `insider` (SF2), `institutional` (SF3), `tickers`, and `sp500`.
        Metadata Cache: Optional ticker metadata cache used for smart batching.
        Data Model: Datatables JSON responses normalized to Polars frames.

    Notes:
        - Pipeline flow: Router -> HttpExecutor -> Writer.
        - Guarantees: Unified rate limiting, structured logging, and error
          accumulation into `RunResult.errors`.
        - Smart batching: Prefetches ticker metadata and estimates rows per ticker
          to keep responses under API row limits.
        - Progress: Spinner or tqdm-based progress tracking based on configuration.
        - Preferred usage: Per-ticker jobs for stability; pagination for `tickers`
          and `sp500` datasets.
    """

    BYTES_PER_TICKER_METADATA = SH_BYTES_PER_TICKER_METADATA
    BYTES_PER_TICKER_FULL = SH_BYTES_PER_TICKER_FULL
    ESTIMATED_TOTAL_TICKERS = SH_ESTIMATED_TOTAL_TICKERS

    def __init__(
        self,
        *,
        api_key: str,
        rate_limit: int,
        schedule: SchedulerConfig | dict[str, Any] | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        concurrency: int | None = None,
        storage: StorageConfig | dict[str, Any] | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
    ) -> None:
        """Initialize the Sharadar client.

        Args:
            api_key: Valid API key for the provider.
            rate_limit: Requests per minute (int).
            schedule: Grouped scheduler configuration for always-on DRR fairness.
            retry: Grouped retry policy configuration.
            throttle: Grouped adaptive throttle policy configuration.
            concurrency: Explicit fetch concurrency limit.
            storage: Grouped data-lifecycle and write-path tuning settings.
            limits: Grouped HTTP connection-pool configuration.
        """
        if not isinstance(api_key, str):
            raise InputError("Sharadar API Key must be a string")
        api_key = api_key.strip()
        if not api_key:
            raise InputError("Sharadar API Key is missing")

        super().__init__(
            api_key=api_key,
            rate_limit=rate_limit,
            schedule=schedule,
            retry=retry,
            throttle=throttle,
            concurrency=concurrency,
            storage=storage,
            limits=limits,
        )

        self._mapper = SchemaMapper()

    def _load_ticker_metadata_from_meta_db(
        self, meta: str | Path | None, symbols: list[str] | None = None
    ) -> pl.DataFrame | None:
        if meta is None:
            return None
        try:
            with duckdb.connect(str(meta), read_only=True) as conn:
                cols = ", ".join(META_REQUIRED_COLUMNS)
                query = f'SELECT {cols} FROM "{DATASET_TABLE["tickers"]}"'  # noqa: S608
                if symbols:
                    placeholders = ", ".join(["?"] * len(symbols))
                    query = f"{query} WHERE ticker IN ({placeholders})"
                    df = conn.execute(query, symbols).pl()
                else:
                    df = conn.execute(query).pl()
        except duckdb.CatalogException as e:
            raise InputError(
                "meta DuckDB must contain sharadar_tickers with ticker, firstpricedate, lastpricedate"
            ) from e
        if df.is_empty():
            return None
        return df.unique(subset=["ticker"], keep="last", maintain_order=True)

    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------
    def get_ticker_info(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch metadata for all or specific tickers (TICKERS).

        Args:
            tickers: Optional list of ticker symbols to filter. If None, fetches all.
            connect_db: Optional DuckDB connection string/path for persistence.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options forwarded to the pipeline.

        Returns:
            pl.DataFrame in memory mode; RunResult when persisting to DuckDB.

        Raises:
            InputError: If parameters (tickers) are invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_ticker_info_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_ticker_info_async(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        if tickers is None:
            cfg = FetchConfig(
                dataset="tickers",
                symbols=None,
                connect_db=connect_db,
                desc="Fetching all tickers metadata",
                table_name=DATASET_TABLE["tickers"],
                progress=progress,
                total_items=None,
                unit=PAGES_UNIT,
                start_date=None,
                end_date=None,
                on_progress=on_progress,
                extra=dict(kwargs),
            )
            return await self._fetch_pagination(cfg)
        if len(tickers) == 0:
            raise InputError("tickers list cannot be empty for SharadarClient.get_ticker_info")
        cfg = FetchConfig(
            dataset="tickers",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching tickers metadata",
            table_name=DATASET_TABLE["tickers"],
            progress=progress,
            total_items=len(tickers),
            unit=TICKERS_UNIT,
            start_date=None,
            end_date=None,
            on_progress=on_progress,
            extra=dict(kwargs),
        )
        return await self._fetch_per_ticker(cfg)

    def get_sp500_history(
        self,
        *,
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch S&P 500 component history.

        Args:
            connect_db: Optional DuckDB connection string/path for persistence.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options forwarded to the pipeline.

        Returns:
            pl.DataFrame in memory mode; RunResult when persisting to DuckDB.

        Raises:
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_sp500_history_async(
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_sp500_history_async(
        self,
        *,
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        cfg = self._build_fetch_config(
            dataset="sp500",
            symbols=None,
            connect_db=connect_db,
            desc="Fetching S&P 500 history",
            table_name=DATASET_TABLE["sp500"],
            total_items=None,
            unit=PAGES_UNIT,
            start_date=None,
            end_date=None,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_pagination(cfg)

    def get_price_data(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Get price data for specified tickers.

        This method delegates to `fetch_per_ticker` to retrieve price data.

        Args:
            tickers: List of ticker symbols to fetch data for.
            meta: Optional DuckDB path containing a sharadar_tickers table for smart batching metadata.
            connect_db: Path to DuckDB database file for storing results.
            start_date: Start date for data fetching (YYYY-MM-DD).
            end_date: End date for data fetching (YYYY-MM-DD).
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional arguments passed to the fetcher.

        Returns:
            polars.DataFrame | RunResult: DataFrame if fetching in-memory,
            or RunResult object if storing to database.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_price_data_async(
                tickers=tickers,
                meta=meta,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_price_data_async(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        self._require_valid_tickers(tickers)
        cfg = self._build_fetch_config(
            dataset="price",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching price data",
            table_name=DATASET_TABLE["price"],
            total_items=None,
            unit=TICKERS_UNIT,
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_per_ticker(cfg, ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers))

    def get_fundamental_data(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        dimension: str = "MRT",
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch fundamental data (SF1).

        Args:
            tickers: List of ticker symbols to fetch.
            meta: Optional DuckDB path containing a sharadar_tickers table for smart batching metadata.
            connect_db: Optional DuckDB connection string/path for persistence.
            start_date: Optional start date filter (YYYY-MM-DD).
            end_date: Optional end date filter (YYYY-MM-DD).
            dimension: SF1 dimension (e.g., 'MRT', 'ARQ', 'ARY').
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            pl.DataFrame in memory mode; RunResult when persisting to DuckDB.
            Rows include SF1 metrics keyed by ticker and calendardate.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_fundamental_data_async(
                tickers=tickers,
                meta=meta,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                dimension=dimension,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_fundamental_data_async(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        dimension: str = "MRT",
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        self._require_valid_tickers(tickers)
        extras = {**dict(kwargs), "dimension": dimension}
        cfg = self._build_fetch_config(
            dataset="fundamental",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching fundamental data",
            table_name=DATASET_TABLE["fundamental"],
            total_items=None,
            unit=TICKERS_UNIT,
            start_date=start_date,
            end_date=end_date,
            extra=extras,
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_per_ticker(cfg, ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers))

    def get_daily_metrics(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch daily metrics (DAILY).

        Args:
            tickers: List of ticker symbols to fetch.
            meta: Optional DuckDB path containing a sharadar_tickers table for smart batching metadata.
            connect_db: Optional DuckDB connection string/path.
            start_date: Optional start date (YYYY-MM-DD).
            end_date: Optional end date (YYYY-MM-DD).
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            pl.DataFrame in memory mode; RunResult when persisting.
            Data includes per-day metrics keyed by ticker and date.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_daily_metrics_async(
                tickers=tickers,
                meta=meta,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_daily_metrics_async(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        self._require_valid_tickers(tickers)
        cfg = self._build_fetch_config(
            dataset="daily",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching daily metrics",
            table_name=DATASET_TABLE["daily"],
            total_items=None,
            unit=TICKERS_UNIT,
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_per_ticker(cfg, ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers))

    def get_corporate_actions(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch corporate actions (ACTIONS).

        Args:
            tickers: List of ticker symbols.
            meta: Optional DuckDB path containing a sharadar_tickers table for smart batching metadata.
            connect_db: Optional DuckDB connection string/path.
            start_date: Optional start date (YYYY-MM-DD).
            end_date: Optional end date (YYYY-MM-DD).
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            pl.DataFrame in memory; RunResult when persisting.
            Rows include dividends/splits keyed by ticker and date.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_corporate_actions_async(
                tickers=tickers,
                meta=meta,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_corporate_actions_async(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        self._require_valid_tickers(tickers)
        cfg = self._build_fetch_config(
            dataset="actions",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching corporate actions",
            table_name=DATASET_TABLE["actions"],
            total_items=None,
            unit=TICKERS_UNIT,
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_per_ticker(cfg, ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers))

    def get_insider_transactions(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch insider trading data (SF2).

        Args:
            tickers: List of ticker symbols.
            meta: Optional DuckDB path containing a sharadar_tickers table for smart batching metadata.
            connect_db: Optional DuckDB connection string/path.
            start_date: Optional start date (YYYY-MM-DD).
            end_date: Optional end date (YYYY-MM-DD).
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            pl.DataFrame in memory; RunResult when persisting.
            Data includes insider transactions keyed by ticker and filingdate.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_insider_transactions_async(
                tickers=tickers,
                meta=meta,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_insider_transactions_async(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        self._require_valid_tickers(tickers)
        cfg = self._build_fetch_config(
            dataset="insider",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching insider trading data",
            table_name=DATASET_TABLE["insider"],
            total_items=None,
            unit=TICKERS_UNIT,
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_per_ticker(cfg, ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers))

    def get_institutional_ownership(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        """Fetch institutional ownership data (SF3).

        Args:
            tickers: List of ticker symbols.
            meta: Optional DuckDB path containing a sharadar_tickers table for smart batching metadata.
            connect_db: Optional DuckDB connection string/path.
            start_date: Optional start date (YYYY-MM-DD).
            end_date: Optional end date (YYYY-MM-DD).
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            pl.DataFrame in memory; RunResult when persisting.
            Data includes institutional positions keyed by ticker and calendardate.

        Raises:
            InputError: If tickers are empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_institutional_ownership_async(
                tickers=tickers,
                meta=meta,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_institutional_ownership_async(
        self,
        *,
        tickers: list[str],
        meta: str | Path | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: object,
    ) -> pl.DataFrame | RunResult:
        self._require_valid_tickers(tickers)
        cfg = self._build_fetch_config(
            dataset="institutional",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching institutional ownership",
            table_name=DATASET_TABLE["institutional"],
            total_items=None,
            unit=TICKERS_UNIT,
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )
        return await self._fetch_per_ticker(cfg, ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers))

    # ----------------------------------------------------------------
    # Internal Data Fetchers
    # ----------------------------------------------------------------

    async def _fetch_per_ticker(
        self,
        config: FetchConfig,
        *,
        ticker_metadata: pl.DataFrame | None = None,
    ) -> pl.DataFrame | RunResult:
        """Fetch data for specific tickers using per-ticker batching.

        This method implements the per-ticker fetching pattern using BaseClient's
        common infrastructure while maintaining Sharadar-specific memory validation.

        Args:
            config: FetchConfig object containing all parameters
            ticker_metadata: Optional preloaded metadata used by the router for smart batching.

        Returns:
            pl.DataFrame for in-memory mode, RunResult for database mode
        """
        symbols = config.symbols

        if symbols is not None and len(symbols) == 0:
            raise InputError("tickers list cannot be empty")
        if symbols:
            self._validate_tickers(symbols)

        bytes_per_item = self.BYTES_PER_TICKER_METADATA if config.dataset == "tickers" else self.BYTES_PER_TICKER_FULL
        self.validate_memory_usage(
            symbols=config.symbols,
            connect_db=config.connect_db,
            bytes_per_item=bytes_per_item,
        )

        pipeline_kwargs: dict[str, JSONValue] = {
            k: v for k, v in dict(config.extra).items() if k not in RESERVED_PIPELINE_KEYS
        }

        result_obj = await self._run_sharadar_pipeline(
            config=config,
            pipeline_kwargs=pipeline_kwargs,
            ticker_metadata=ticker_metadata,
        )
        return result_obj

    async def _fetch_pagination(self, config: FetchConfig) -> pl.DataFrame | RunResult:
        """Fetch full dataset via pagination (e.g., SP500, All Tickers).

        This method implements pagination using BaseClient infrastructure with
        Sharadar-specific handling for large datasets.

        Args:
            config: FetchConfig containing dataset, symbols, connect_db, desc,
                table_name, progress, total_items, unit, start_date, end_date, and extra.

        Returns:
            pl.DataFrame for in-memory mode, RunResult for database mode.
        """

        pipeline_kwargs: dict[str, JSONValue] = {
            k: v for k, v in dict(config.extra).items() if k not in RESERVED_PIPELINE_KEYS
        }

        result_obj = await self._run_sharadar_pipeline(
            config=config,
            pipeline_kwargs=pipeline_kwargs,
        )
        return result_obj

    async def _run_sharadar_pipeline(
        self,
        *,
        config: FetchConfig,
        pipeline_kwargs: dict[str, JSONValue],
        ticker_metadata: pl.DataFrame | None = None,
    ) -> pl.DataFrame | RunResult:
        async with self.managed_writer(config.connect_db, show_progress=config.progress) as writer:
            router = create_router(
                "sharadar",
                api_key=cast("str", self.api_key),
                rate_limit=self._config.requests_per_minute,
                start_date=config.start_date,
                end_date=config.end_date,
                ticker_metadata=ticker_metadata,
            )

            await self.run_pipeline(
                router=router,
                dataset=config.dataset,
                symbols=config.symbols,
                writer=writer,
                mapper=self._mapper,
                on_progress=config.on_progress,
                progress=config.progress,
                **pipeline_kwargs,
            )

            result_obj = await self.collect_results(
                writer=writer,
                table_name=config.table_name,
                connect_db=config.connect_db,
            )
            return result_obj

    def _build_fetch_config(
        self,
        *,
        dataset: SharadarDataset,
        symbols: list[str] | None,
        connect_db: str | Path | None,
        desc: str,
        table_name: str,
        total_items: int | None = None,
        unit: str = TICKERS_UNIT,
        start_date: str | None = None,
        end_date: str | None = None,
        extra: dict[str, Any] | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
    ) -> FetchConfig:
        computed_total = total_items
        if computed_total is None and symbols is not None:
            computed_total = len(symbols)
        return FetchConfig(
            dataset=dataset,
            symbols=symbols,
            connect_db=connect_db,
            desc=desc,
            table_name=table_name,
            progress=progress,
            total_items=computed_total,
            unit=unit,
            start_date=start_date,
            end_date=end_date,
            on_progress=on_progress,
            extra=dict(extra or {}),
        )

    def _validate_tickers(self, tickers: list[str]) -> None:
        """
        Validate a list of ticker symbols.
        Enforces per-item rules: non-empty, no whitespace-only, allowed characters.
        Raises InputError on first invalid ticker.
        """
        validate_tickers(tickers)
        for t in tickers:
            if not TICKER_PATTERN.match(t):
                raise InputError(f"Ticker '{t}' contains invalid characters")

    def _require_valid_tickers(self, tickers: list[str]) -> None:
        if not tickers:
            raise InputError("tickers list cannot be empty")
        self._validate_tickers(tickers)
