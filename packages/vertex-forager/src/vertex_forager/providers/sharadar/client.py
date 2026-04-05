from __future__ import annotations

import logging
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, Literal, cast

import duckdb
import polars as pl

from vertex_forager.clients.base import BaseClient
from vertex_forager.constants import RESERVED_PIPELINE_KEYS
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
from vertex_forager.utils import make_sync, validate_tickers

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
        quality_check: Literal["warn", "error"] = "warn",
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
            quality_check: Data quality violation handling mode.
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
            quality_check=quality_check,
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
    async def _get_ticker_info_async(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch metadata for all or specific tickers (TICKERS).

        Args:
            tickers: Optional list of ticker symbols to filter. If None, fetches all.
            connect_db: Optional DuckDB connection string/path for persistence.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options forwarded to the pipeline.

        Returns:
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If parameters (tickers) are invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        if tickers is not None and len(tickers) == 0:
            raise InputError("tickers list cannot be empty for SharadarClient.get_ticker_info")
        return await self._dispatch_fetch(
            dataset="tickers",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["tickers"],
            start_date=None,
            end_date=None,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
        )

    get_ticker_info = make_sync(_get_ticker_info_async)

    async def _get_sp500_history_async(
        self,
        *,
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch S&P 500 component history.

        Args:
            connect_db: Optional DuckDB connection string/path for persistence.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options forwarded to the pipeline.

        Returns:
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        pipeline_kwargs: dict[str, JSONValue] = {
            k: v for k, v in dict(kwargs).items() if k not in RESERVED_PIPELINE_KEYS
        }
        return await self._run_sharadar_pipeline(
            dataset="sp500",
            symbols=None,
            connect_db=connect_db,
            table_name=DATASET_TABLE["sp500"],
            pipeline_kwargs=pipeline_kwargs,
            ticker_metadata=None,
            start_date=None,
            end_date=None,
            progress=progress,
            on_progress=on_progress,
        )

    get_sp500_history = make_sync(_get_sp500_history_async)

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
        **kwargs: Any,
    ) -> RunResult:
        """Get price data for specified tickers.

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
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        self._require_valid_tickers(tickers)
        return await self._dispatch_fetch(
            dataset="price",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["price"],
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
            ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers),
        )

    get_price_data = make_sync(_get_price_data_async)

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
        **kwargs: Any,
    ) -> RunResult:
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
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        self._require_valid_tickers(tickers)
        extras = {**dict(kwargs), "dimension": dimension}
        return await self._dispatch_fetch(
            dataset="fundamental",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["fundamental"],
            start_date=start_date,
            end_date=end_date,
            extra=extras,
            progress=progress,
            on_progress=on_progress,
            ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers),
        )

    get_fundamental_data = make_sync(_get_fundamental_data_async)

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
        **kwargs: Any,
    ) -> RunResult:
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
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        self._require_valid_tickers(tickers)
        return await self._dispatch_fetch(
            dataset="daily",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["daily"],
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
            ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers),
        )

    get_daily_metrics = make_sync(_get_daily_metrics_async)

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
        **kwargs: Any,
    ) -> RunResult:
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
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        self._require_valid_tickers(tickers)
        return await self._dispatch_fetch(
            dataset="actions",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["actions"],
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
            ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers),
        )

    get_corporate_actions = make_sync(_get_corporate_actions_async)

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
        **kwargs: Any,
    ) -> RunResult:
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
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        self._require_valid_tickers(tickers)
        return await self._dispatch_fetch(
            dataset="insider",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["insider"],
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
            ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers),
        )

    get_insider_transactions = make_sync(_get_insider_transactions_async)

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
        **kwargs: Any,
    ) -> RunResult:
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
            RunResult. In-memory payload is available via `result.data`.

        Raises:
            InputError: If tickers are empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        self._require_valid_tickers(tickers)
        return await self._dispatch_fetch(
            dataset="institutional",
            symbols=tickers,
            connect_db=connect_db,
            table_name=DATASET_TABLE["institutional"],
            start_date=start_date,
            end_date=end_date,
            extra=dict(kwargs),
            progress=progress,
            on_progress=on_progress,
            ticker_metadata=self._load_ticker_metadata_from_meta_db(meta, tickers),
        )

    get_institutional_ownership = make_sync(_get_institutional_ownership_async)

    # ----------------------------------------------------------------
    # Internal Data Fetchers
    # ----------------------------------------------------------------

    async def _dispatch_fetch(
        self,
        *,
        dataset: SharadarDataset,
        symbols: list[str] | None,
        connect_db: str | Path | None,
        table_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
        extra: dict[str, Any] | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        ticker_metadata: pl.DataFrame | None = None,
    ) -> RunResult:
        if symbols is not None and len(symbols) == 0:
            raise InputError("tickers list cannot be empty")
        symbols_provided = symbols is not None and len(symbols) > 0
        if symbols_provided:
            self._validate_tickers(symbols)  # type: ignore[arg-type]
            bytes_per_item = self.BYTES_PER_TICKER_METADATA if dataset == "tickers" else self.BYTES_PER_TICKER_FULL
            self.validate_memory_usage(symbols=symbols, connect_db=connect_db, bytes_per_item=bytes_per_item)

        pipeline_kwargs: dict[str, JSONValue] = {
            k: v for k, v in dict(extra or {}).items() if k not in RESERVED_PIPELINE_KEYS
        }

        return await self._run_sharadar_pipeline(
            dataset=dataset,
            symbols=symbols,
            connect_db=connect_db,
            table_name=table_name,
            pipeline_kwargs=pipeline_kwargs,
            ticker_metadata=ticker_metadata,
            start_date=start_date,
            end_date=end_date,
            progress=progress,
            on_progress=on_progress,
        )

    async def _run_sharadar_pipeline(
        self,
        *,
        dataset: SharadarDataset,
        symbols: list[str] | None,
        connect_db: str | Path | None,
        table_name: str,
        pipeline_kwargs: dict[str, JSONValue],
        ticker_metadata: pl.DataFrame | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
    ) -> RunResult:
        async with self.managed_writer(connect_db, show_progress=progress) as writer:
            router = create_router(
                "sharadar",
                api_key=cast("str", self.api_key),
                rate_limit=self._config.requests_per_minute,
                start_date=start_date,
                end_date=end_date,
                ticker_metadata=ticker_metadata,
            )

            await self.run_pipeline(
                router=router,
                dataset=dataset,
                symbols=symbols,
                writer=writer,
                mapper=self._mapper,
                on_progress=on_progress,
                progress=progress,
                **pipeline_kwargs,
            )

            result_obj = await self.collect_results(
                writer=writer,
                table_name=table_name,
                connect_db=connect_db,
            )
            return result_obj

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
