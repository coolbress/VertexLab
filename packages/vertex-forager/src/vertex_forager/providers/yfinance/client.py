from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, cast

import polars as pl

from vertex_forager.clients.base import BaseClient
from vertex_forager.constants import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRY_BASE_BACKOFF_S,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_RETRY_MAX_BACKOFF_S,
    MEM_THRESHOLD_ABS_MB,
    MEM_THRESHOLD_RATIO,
    RESERVED_PIPELINE_KEYS,
)
from vertex_forager.core.config import (
    AdaptiveThrottleConfig,
    AdvancedConfig,
    HTTPConfig,
    RetryConfig,
    RunResult,
    SchedulerConfig,
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
from vertex_forager.utils import run_sync_compat, validate_memory_usage, validate_tickers

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from vertex_forager.core.config import ProgressSnapshot

logger = logging.getLogger(__name__)


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
        adaptive_throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        concurrency: int | None = None,
        flush_threshold_rows: int | None = None,
        checkpoint_retention_days: int | None = None,
        run_history_retention_days: int | None = None,
        http_timeout_s: float | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
        advanced: AdvancedConfig | dict[str, Any] | None = None,
    ) -> None:
        """Initialize the YFinance client.

        Args:
            api_key (str | None): Unused for yfinance. Kept for API symmetry. Default is None.
            rate_limit (int): Requests per minute cap. Default is 60.
            schedule: Grouped scheduler configuration for always-on DRR fairness.
            retry (RetryConfig | dict[str, Any] | None): Retry settings for fetch failures. Default is None.
            adaptive_throttle (
                AdaptiveThrottleConfig | dict[str, Any] | None,
            ): Adaptive throttle settings. Default is None.
            concurrency (int | None): Maximum concurrent fetch workers. Default is None.
            flush_threshold_rows (int | None): Buffered row threshold before writer flush. Default is None.
            checkpoint_retention_days (int | None): Days to retain completed checkpoint state before pruning.
                Default is None, which resolves to the runtime default of 7 days.
            run_history_retention_days (int | None): Days to retain persisted run history before pruning.
                Default is None, which resolves to the runtime default of 90 days.
            http_timeout_s (float | None): HTTP request timeout in seconds. Default is None.
            limits (HTTPConfig | dict[str, Any] | None): HTTP connection pool settings. Default is None.
            advanced (AdvancedConfig | dict[str, Any] | None): Advanced runtime options. Default is None.
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
        if retry is None:
            retry = RetryConfig(
                max_attempts=DEFAULT_RETRY_MAX_ATTEMPTS,
                base_backoff_s=DEFAULT_RETRY_BASE_BACKOFF_S,
                max_backoff_s=DEFAULT_RETRY_MAX_BACKOFF_S,
            )
        super().__init__(
            api_key=None,
            rate_limit=normalized,
            schedule=schedule,
            retry=retry,
            adaptive_throttle=adaptive_throttle,
            concurrency=concurrency,
            flush_threshold_rows=flush_threshold_rows,
            checkpoint_retention_days=checkpoint_retention_days,
            run_history_retention_days=run_history_retention_days,
            http_timeout_s=http_timeout_s,
            limits=limits,
            advanced=advanced,
        )
        self._mapper = SchemaMapper()

    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------

    # --- Reference Data ---

    def get_info(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker metadata/info.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string/path for persistence.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_info_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_info_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="info",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_info",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Market Data ---

    def get_price_data(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
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
            Polars DataFrame in memory or RunResult when persisting.

        Notes:
            Date filters (start/end) apply to price only; other datasets ignore date filters.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_price_data_async(
                tickers=tickers,
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
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="price",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_price",
            start_date=start_date,
            end_date=end_date,
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Financials ---

    def get_financials(
        self,
        *,
        kind: Literal["balance_sheet", "income_stmt", "cashflow", "earnings"] = "income_stmt",
        period: Literal["annual", "quarterly"] = "annual",
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch financial statements (Unified Method).

        Args:
            kind: Type of financial statement ('balance_sheet', 'income_stmt', 'cashflow', 'earnings').
            period: Reporting period ('annual' or 'quarterly'). For 'earnings', quarterly is deprecated.
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

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
        return run_sync_compat(
            self._get_financials_async(
                kind=kind,
                period=period,
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

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
    ) -> pl.DataFrame | RunResult:
        target_kind = "financials" if kind in ("income_stmt", "earnings") else kind
        if kind == "earnings":
            logger.warning(
                "YFinance 'earnings' maps to 'financials' (income statements). Annual earnings requests are redirected."
            )
        if kind == "earnings" and period == "quarterly":
            raise InputError("quarterly_earnings is deprecated in yfinance; use income_stmt with period='quarterly'.")
        dataset = f"quarterly_{target_kind}" if period == "quarterly" else target_kind

        return await self._dispatch_fetch(
            dataset=cast("YFinanceDataset", dataset),
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_financials",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Corporate Actions ---

    def get_actions(
        self,
        *,
        kind: Literal["dividends", "splits"] = "dividends",
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch corporate actions (Unified Method: dividends or splits).

        Args:
            kind: Type of action ('dividends' or 'splits').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            start_date: Optional start date (YYYY-MM-DD).
            end_date: Optional end date (YYYY-MM-DD).
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_actions_async(
                kind=kind,
                tickers=tickers,
                connect_db=connect_db,
                start_date=start_date,
                end_date=end_date,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_actions_async(
        self,
        *,
        kind: Literal["dividends", "splits"] = "dividends",
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset=cast("YFinanceDataset", kind),
            tickers=tickers,
            connect_db=connect_db,
            table_name=f"yfinance_{kind}",
            start_date=start_date,
            end_date=end_date,
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Holders ---

    def get_holders(
        self,
        *,
        kind: Literal["institutional", "mutualfund"] = "institutional",
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch holders information (Unified Method).

        Args:
            kind: Type of holder ('institutional', 'mutualfund').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_holders_async(
                kind=kind,
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_holders_async(
        self,
        *,
        kind: Literal["institutional", "mutualfund"] = "institutional",
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        dataset = f"{kind}_holders"
        return await self._dispatch_fetch(
            dataset=cast("YFinanceDataset", dataset),
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_holders",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    def get_major_holders(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch major holders summary metrics.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_major_holders_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_major_holders_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="major_holders",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_major_holders",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Insider ---

    def get_insider_roster_holders(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch insider roster holders.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_insider_roster_holders_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_insider_roster_holders_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="insider_roster_holders",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_insider_roster_holders",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    def get_insider_purchases(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch insider purchases.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_insider_purchases_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_insider_purchases_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="insider_purchases",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_insider_purchases",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Calendar ---

    def get_calendar(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch earnings calendar.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_calendar_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_calendar_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="calendar",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_calendar",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- Analyst Recommendations ---

    def get_recommendations(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch analyst recommendations.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_recommendations_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_recommendations_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="recommendations",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_recommendations",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # --- news ---

    def get_news(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker news.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            progress: Whether to display built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return run_sync_compat(
            self._get_news_async(
                tickers=tickers,
                connect_db=connect_db,
                progress=progress,
                on_progress=on_progress,
                **kwargs,
            )
        )

    async def _get_news_async(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._dispatch_fetch(
            dataset="news",
            tickers=tickers,
            connect_db=connect_db,
            table_name="yfinance_news",
            progress=progress,
            on_progress=on_progress,
            **kwargs,
        )

    # ----------------------------------------------------------------
    # Internal Data Fetchers
    # ----------------------------------------------------------------

    async def _fetch_per_ticker(
        self,
        *,
        dataset: YFinanceDataset,
        symbols: list[str],
        connect_db: str | Path | None,
        table_name: str,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch data for specific tickers using per-ticker batching.

        This method implements the per-ticker fetching pattern using BaseClient's
        common infrastructure while maintaining YFinance-specific logic for
        memory validation and rate limiting optimization.

        YFinance-specific characteristics:
        - Free API with no authentication required
        - Conservative rate limiting to avoid IP bans (60 req/min default)
        - Compact data format (smaller memory footprint than premium APIs)
        - Limited ticker universe compared to institutional providers

        Args:
            dataset: Dataset name (e.g., "price", "financials", "info")
            symbols: List of symbols to fetch (required; must be non-empty)
            connect_db: Database connection string/path, or None for in-memory
            table_name: Table name for result collection
            progress: Whether to show built-in progress output.
            on_progress: Optional callback receiving ProgressSnapshot updates.
            start_date: Start date for data fetch
            end_date: End date for data fetch
            **kwargs: Additional provider-specific arguments

        Returns:
            pl.DataFrame for in-memory mode, RunResult for database mode
        """
        validate_tickers(symbols)
        bytes_per_item = YF_SIZE_MAP.get(dataset, DEFAULT_BYTES_PER_ITEM)
        validate_memory_usage(
            symbols=symbols,
            connect_db=connect_db,
            bytes_per_item=bytes_per_item,
            threshold_ratio=MEM_THRESHOLD_RATIO,
            threshold_absolute=MEM_THRESHOLD_ABS_MB * 1024 * 1024,
        )

        result_obj: pl.DataFrame | RunResult = (
            RunResult(provider="yfinance") if connect_db is not None else pl.DataFrame()
        )
        async with self.managed_writer(connect_db, show_progress=progress) as writer:
            router = create_router(
                "yfinance",
                api_key=self.api_key or "",
                rate_limit=self._config.requests_per_minute,
                start_date=start_date,
                end_date=end_date,
                **{k: v for k, v in kwargs.items() if k in {PRICE_BATCH_SIZE_KEY}},
            )

            await self.run_pipeline(
                router=router,
                dataset=dataset,
                symbols=symbols,
                writer=writer,
                mapper=self._mapper,
                on_progress=on_progress,
                progress=progress,
                **{k: v for k, v in kwargs.items() if k not in (RESERVED_PIPELINE_KEYS | {PRICE_BATCH_SIZE_KEY})},
            )

            result_obj = await self.collect_results(
                writer=writer,
                table_name=table_name,
                connect_db=connect_db,
            )
        return result_obj

    async def _dispatch_fetch(
        self,
        *,
        dataset: YFinanceDataset,
        tickers: list[str],
        connect_db: str | Path | None,
        table_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
        progress: bool = False,
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        return await self._fetch_per_ticker(
            dataset=dataset,
            symbols=tickers,
            connect_db=connect_db,
            table_name=table_name,
            progress=progress,
            on_progress=on_progress,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
