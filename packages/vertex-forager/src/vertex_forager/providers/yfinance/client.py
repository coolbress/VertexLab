from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from vertex_forager.clients.base import BaseClient
from vertex_forager.constants import (
    DEFAULT_RATE_LIMIT,
    MEM_THRESHOLD_ABS_MB,
    MEM_THRESHOLD_RATIO,
    RESERVED_PIPELINE_KEYS,
)
from vertex_forager.core.config import (
    AdaptiveThrottleConfig,
    HTTPConfig,
    RetryConfig,
    RunResult,
    SchedulerConfig,
    StorageConfig,
)
from vertex_forager.core.types import YFinanceDataset
from vertex_forager.exceptions import InputError
from vertex_forager.logging.constants import (
    CLIENT_LOG_PREFIX,
    LOG_RATE_LIMIT_EXCEEDS_DEFAULT,
    LOG_RATE_LIMIT_INVALID_INT,
    LOG_RATE_LIMIT_INVALID_TYPE,
)
from vertex_forager.providers.yfinance.constants import (
    DEFAULT_BYTES_PER_ITEM,
    PRICE_BATCH_SIZE_KEY,
)
from vertex_forager.providers.yfinance.constants import SIZE_MAP as YF_SIZE_MAP
from vertex_forager.routers import create_router
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.schema.registry import get_dataset_spec
from vertex_forager.utils import make_sync, validate_memory_usage, validate_tickers

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from vertex_forager.core.checkpoint import Checkpoint
    from vertex_forager.core.config import ProgressSnapshot

logger = logging.getLogger(__name__)


def _normalize_pickle_compat_datasets(value: list[str] | None) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise InputError("pickle_compat_datasets must be a list of strings")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise InputError("pickle_compat_datasets must contain only strings")
        dataset = item.strip()
        if dataset and dataset not in normalized:
            normalized.append(dataset)
    return normalized or None


class YFinanceClient(BaseClient[YFinanceDataset]):
    """Client for Yahoo Finance datasets via yfinance.

    This client integrates yfinance with the VertexForager pipeline to provide
    consistent rate limiting, logging, and error handling across datasets.

    Attributes:
        Free API: No authentication required.
        Rate Limiting: Default 60 requests per minute to avoid IP bans.
        Data Quality: Real-time data may be delayed; historical accuracy is best-effort.
        Limitations: Financial statements expose only recent periods (no full-history).

    Notes:
        - Pipeline flow: Router -> HttpExecutor -> Writer.
        - Guarantees: Unified rate limiting, structured logging, and error propagation.
        - Preferred usage: Per-ticker jobs for stability; bulk price downloads avoided.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        schedule: SchedulerConfig | dict[str, Any] | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        quality_check: Literal["warn", "error"] = "warn",
        pickle_compat_datasets: list[str] | None = None,
        concurrency: int | None = None,
        storage: StorageConfig | dict[str, Any] | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
    ) -> None:
        """Initialize the YFinance client.

        Args:
            api_key (str | None): Unused for yfinance. Kept for API symmetry. Default is None.
            rate_limit (int): Requests per minute cap. Default is 60.
            schedule: Grouped scheduler configuration for always-on DRR fairness.
            retry (RetryConfig | dict[str, Any] | None): Retry settings for fetch failures. Default is None.
            throttle (
                AdaptiveThrottleConfig | dict[str, Any] | None,
            ): Adaptive throttle settings. Default is None.
            quality_check: Data quality violation handling mode.
            pickle_compat_datasets: Dataset allowlist for YFinance pickle compatibility fallback.
            concurrency (int | None): Maximum concurrent fetch workers. Default is None.
            storage: Grouped data-lifecycle and write-path tuning settings.
            limits (HTTPConfig | dict[str, Any] | None): HTTP connection pool settings. Default is None.
        """
        normalized = rate_limit
        if isinstance(normalized, int):
            if normalized <= 0:
                logger.warning(LOG_RATE_LIMIT_INVALID_INT.format(prefix=CLIENT_LOG_PREFIX, value=normalized))
                normalized = DEFAULT_RATE_LIMIT
            elif normalized > DEFAULT_RATE_LIMIT:
                logger.warning(LOG_RATE_LIMIT_EXCEEDS_DEFAULT.format(prefix=CLIENT_LOG_PREFIX, value=normalized))
        else:
            logger.warning(LOG_RATE_LIMIT_INVALID_TYPE.format(prefix=CLIENT_LOG_PREFIX, value=normalized))
            normalized = DEFAULT_RATE_LIMIT
        self._pickle_compat_datasets = _normalize_pickle_compat_datasets(pickle_compat_datasets)
        if retry is None:
            retry = RetryConfig()
        super().__init__(
            api_key=None,
            rate_limit=normalized,
            schedule=schedule,
            retry=retry,
            throttle=throttle,
            quality_check=quality_check,
            concurrency=concurrency,
            storage=storage,
            limits=limits,
        )
        self._mapper = SchemaMapper()

    def _table_name_for_dataset(self, dataset: YFinanceDataset) -> str:
        spec = get_dataset_spec("yfinance", dataset)
        if spec is None:
            raise InputError(f"Unsupported YFinance dataset: {dataset}")
        return spec.schema.table

    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------

    # --- Reference Data ---

    async def _get_info_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch ticker metadata/info.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string/path for persistence.
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
        dataset: YFinanceDataset = "info"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_info = make_sync(_get_info_async)

    async def _get_fast_info_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch lightweight metadata from the yfinance ``fast_info`` dataset.

        Args:
            tickers: Ticker symbols to collect.
            connect_db: Optional DuckDB destination. If omitted, results stay in memory.
            progress: Whether to emit progress updates during collection.
            on_progress: Optional callback receiving progress snapshots.
            **kwargs: Additional pipeline/router options forwarded unchanged.

        Returns:
            RunResult for the `fast_info` collection run.

        Notes:
            The `fast_info` dataset is intentionally lightweight and may expose
            a flexible per-ticker field set compared with the fuller `info`
            dataset.
        """
        dataset: YFinanceDataset = "fast_info"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_fast_info = make_sync(_get_fast_info_async)

    # --- Market Data ---

    async def _get_price_data_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch historical price data (OHLCV).

        Args:
            tickers: List of ticker symbols.
            connect_db: Database connection string.
            start_date: Start date (YYYY-MM-DD). If None, fetches all available history.
            end_date: End date (YYYY-MM-DD). If None, fetches up to latest available date.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options forwarded to the pipeline/executor.

        Returns:
            RunResult. In-memory payload is available via `result.data`.

        Notes:
            Date filters (start/end) apply to price only; other datasets ignore date filters.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        dataset: YFinanceDataset = "price"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            start_date=start_date,
            end_date=end_date,
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_price_data = make_sync(_get_price_data_async)

    # --- Financials ---

    async def _get_financials_async(
        self,
        *,
        kind: Literal["balance_sheet", "income_stmt", "cashflow", "earnings"] = "income_stmt",
        period: Literal["annual", "quarterly"] = "annual",
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch financial statements (Unified Method).

        Args:
            kind: Type of financial statement ('balance_sheet', 'income_stmt', 'cashflow', 'earnings').
            period: Reporting period ('annual' or 'quarterly'). For 'earnings', quarterly is deprecated.
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.

        Returns:
            RunResult. In-memory payload is available via `result.data`.

        Notes:
            - yfinance provides only recent periods for financials (e.g., last few years/quarters).
            - Date filters (start/end) are not applicable to financials endpoints.
            - Period selects quarterly vs annual datasets. yfinance no longer supports 'quarterly_earnings'.
            - For full historical coverage, prefer institutional sources like Sharadar SF1 (ARY/ARQ).

        Raises:
            InputError: If requesting deprecated quarterly earnings or tickers list is invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        target_kind = "financials" if kind in ("income_stmt", "earnings") else kind
        if kind == "earnings":
            logger.warning(
                "YFinance 'earnings' maps to 'financials' (income statements). Annual earnings requests are redirected."
            )
        if kind == "earnings" and period == "quarterly":
            raise InputError("quarterly_earnings is deprecated in yfinance; use income_stmt with period='quarterly'.")
        dataset: YFinanceDataset = f"quarterly_{target_kind}" if period == "quarterly" else target_kind  # type: ignore[assignment]

        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_financials = make_sync(_get_financials_async)

    # --- Corporate Actions ---

    async def _get_actions_async(
        self,
        *,
        kind: Literal["actions", "dividends", "splits"] = "dividends",
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch corporate actions (Unified Method: dividends or splits).

        Args:
            kind: Type of action ('actions', 'dividends', or 'splits').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = kind
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            start_date=start_date,
            end_date=end_date,
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_actions = make_sync(_get_actions_async)

    # --- Holders ---

    async def _get_holders_async(
        self,
        *,
        kind: Literal["institutional", "mutualfund"] = "institutional",
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch holders information (Unified Method).

        Args:
            kind: Type of holder ('institutional', 'mutualfund').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = f"{kind}_holders"  # type: ignore[assignment]
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_holders = make_sync(_get_holders_async)

    async def _get_major_holders_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch major holders summary metrics.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = "major_holders"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_major_holders = make_sync(_get_major_holders_async)

    # --- Insider ---

    async def _get_insider_roster_holders_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch insider roster holders.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = "insider_roster_holders"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_insider_roster_holders = make_sync(_get_insider_roster_holders_async)

    async def _get_insider_purchases_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch insider purchases.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = "insider_purchases"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_insider_purchases = make_sync(_get_insider_purchases_async)

    # --- Calendar ---

    async def _get_calendar_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch earnings calendar.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = "calendar"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_calendar = make_sync(_get_calendar_async)

    # --- Analyst Recommendations ---

    async def _get_recommendations_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch analyst recommendations.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = "recommendations"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_recommendations = make_sync(_get_recommendations_async)

    # --- news ---

    async def _get_news_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Fetch ticker news.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
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
        dataset: YFinanceDataset = "news"
        return await self._dispatch_fetch(
            dataset=dataset,
            tickers=tickers,
            connect_db=connect_db,
            table_name=self._table_name_for_dataset(dataset),
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    get_news = make_sync(_get_news_async)

    # ----------------------------------------------------------------
    # Internal Data Fetchers
    # ----------------------------------------------------------------

    async def _dispatch_fetch(
        self,
        *,
        dataset: YFinanceDataset,
        tickers: list[str],
        connect_db: str | Path | None,
        table_name: str,
        checkpoint: Checkpoint | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        """Dispatch a fetch request to the YFinance pipeline.

        This is the single internal entry point for all YFinance async fetch operations.
        It validates inputs, checks memory safety, and delegates to the pipeline.

        Args:
            dataset: YFinance dataset name (e.g., "price", "financials", "info").
            tickers: List of ticker symbols to fetch (required; must be non-empty).
            connect_db: Database connection string/path, or None for in-memory.
            table_name: Table name for result collection.
            start_date: Start date for data fetch.
            end_date: End date for data fetch.
            progress: Whether to show built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific arguments.

        Returns:
            RunResult for both in-memory and database modes.

        Raises:
            InputError: If tickers list is empty or contains invalid symbols.
        """
        validate_tickers(tickers)
        bytes_per_item = YF_SIZE_MAP.get(dataset, DEFAULT_BYTES_PER_ITEM)
        validate_memory_usage(
            symbols=tickers,
            connect_db=connect_db,
            bytes_per_item=bytes_per_item,
            threshold_ratio=MEM_THRESHOLD_RATIO,
            threshold_absolute=MEM_THRESHOLD_ABS_MB * 1024 * 1024,
        )

        async with self.managed_writer(connect_db, show_progress=progress) as writer:
            router = create_router(
                "yfinance",
                api_key=self.api_key or "",
                rate_limit=self._config.requests_per_minute,
                start_date=start_date,
                end_date=end_date,
                pickle_compat_datasets=self._pickle_compat_datasets,
                **{k: v for k, v in kwargs.items() if k in {PRICE_BATCH_SIZE_KEY}},
            )

            await self.run_pipeline(
                router=router,
                dataset=dataset,
                symbols=tickers,
                writer=writer,
                mapper=self._mapper,
                on_progress=on_progress,
                progress=progress,
                checkpoint=checkpoint,
                table_name=table_name,
                **{k: v for k, v in kwargs.items() if k not in (RESERVED_PIPELINE_KEYS | {PRICE_BATCH_SIZE_KEY})},
            )

            result_obj = await self.collect_results(
                writer=writer,
                table_name=table_name,
                connect_db=connect_db,
            )
        return result_obj

    async def _resume_dataset_async(
        self,
        *,
        dataset: str,
        checkpoint: Checkpoint,
        output: str,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> RunResult:
        symbols_raw = checkpoint.meta.get("requested_symbols", [])
        tickers = [str(symbol) for symbol in symbols_raw] if isinstance(symbols_raw, list) else []
        if not tickers:
            raise InputError("Checkpoint resume requires stored requested_symbols metadata")
        dataset_name: YFinanceDataset = dataset  # type: ignore[assignment]
        return await self._dispatch_fetch(
            dataset=dataset_name,
            tickers=tickers,
            connect_db=output,
            table_name=self._table_name_for_dataset(dataset_name),
            checkpoint=checkpoint,
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )
