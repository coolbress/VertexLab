from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

import polars as pl
from vertex_forager.clients.base import BaseClient
from vertex_forager.constants import (
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRY_BASE_BACKOFF_S,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_RETRY_MAX_BACKOFF_S,
    RESERVED_PIPELINE_KEYS,
)
from vertex_forager.core.config import RetryConfig, RunResult
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
from vertex_forager.utils import jupyter_safe, validate_memory_usage, validate_tickers

if TYPE_CHECKING:
    from pathlib import Path

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
        **kwargs: Any,
    ) -> None:
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
        if "retry" not in kwargs:
            kwargs["retry"] = RetryConfig(
                max_attempts=DEFAULT_RETRY_MAX_ATTEMPTS,
                base_backoff_s=DEFAULT_RETRY_BASE_BACKOFF_S,
                max_backoff_s=DEFAULT_RETRY_MAX_BACKOFF_S,
            )
        # Ensure api_key is None for YFinanceClient
        super().__init__(api_key=None, rate_limit=normalized, **kwargs)
        self._mapper = SchemaMapper()

    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------

    # --- Reference Data ---

    @jupyter_safe
    async def get_info(
        self, *, tickers: list[str], connect_db: str | Path | None = None, show_progress: bool = True, **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker metadata/info.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string/path for persistence.
            show_progress: Whether to display progress indicators (default: True).
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="info",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance info",
            table_name="yfinance_info",
            show_progress=show_progress,
            **kwargs,
        )

    # --- Market Data ---

    @jupyter_safe
    async def get_price_data(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch historical price data (OHLCV).

        Args:
            tickers: List of ticker symbols.
            connect_db: Database connection string.
            start_date: Start date (YYYY-MM-DD). If None, fetches all available history.
            end_date: End date (YYYY-MM-DD). If None, fetches up to latest available date.
            show_progress: Whether to display progress indicators (default: True).
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
        return await self._dispatch_fetch(
            dataset="price",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance price data",
            table_name="yfinance_price",
            start_date=start_date,
            end_date=end_date,
            show_progress=show_progress,
            **kwargs,
        )

    # --- Financials ---

    @jupyter_safe
    async def get_financials(
        self,
        *,
        kind: Literal["balance_sheet", "income_stmt", "cashflow", "earnings"] = "income_stmt",
        period: Literal["annual", "quarterly"] = "annual",
        tickers: list[str],
        connect_db: str | Path | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch financial statements (Unified Method).

        Args:
            kind: Type of financial statement ('balance_sheet', 'income_stmt', 'cashflow', 'earnings').
            period: Reporting period ('annual' or 'quarterly'). For 'earnings', quarterly is deprecated.
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.

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
        # Map 'income_stmt' alias to 'financials' and prevent deprecated quarterly_earnings.
        target_kind = "financials" if kind in ("income_stmt", "earnings") else kind
        if kind == "earnings":
            logger.warning(
                "YFinance 'earnings' maps to 'financials' (income statements). Annual earnings requests are redirected."
            )
        if kind == "earnings" and period == "quarterly":
            raise InputError("quarterly_earnings is deprecated in yfinance; use income_stmt with period='quarterly'.")
        dataset = f"quarterly_{target_kind}" if period == "quarterly" else target_kind
        from typing import cast

        return await self._dispatch_fetch(
            dataset=cast("YFinanceDataset", dataset),
            tickers=tickers,
            connect_db=connect_db,
            desc=f"Fetching YFinance {dataset}",
            table_name="yfinance_financials",
            show_progress=show_progress,
            **kwargs,
        )

    # --- Corporate Actions ---

    @jupyter_safe
    async def get_actions(
        self,
        *,
        kind: Literal["dividends", "splits"] = "dividends",
        tickers: list[str],
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch corporate actions (Unified Method: dividends or splits).

        Args:
            kind: Type of action ('dividends' or 'splits').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            start_date: Optional start date (YYYY-MM-DD).
            end_date: Optional end date (YYYY-MM-DD).
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        from typing import cast

        return await self._dispatch_fetch(
            dataset=cast("YFinanceDataset", kind),
            tickers=tickers,
            connect_db=connect_db,
            desc=f"Fetching YFinance {kind}",
            table_name=f"yfinance_{kind}",
            start_date=start_date,
            end_date=end_date,
            show_progress=show_progress,
            **kwargs,
        )

    # --- Holders ---

    @jupyter_safe
    async def get_holders(
        self,
        *,
        kind: Literal["institutional", "mutualfund"] = "institutional",
        tickers: list[str],
        connect_db: str | Path | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch holders information (Unified Method).

        Args:
            kind: Type of holder ('institutional', 'mutualfund').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        dataset = f"{kind}_holders"
        from typing import cast

        return await self._dispatch_fetch(
            dataset=cast("YFinanceDataset", dataset),
            tickers=tickers,
            connect_db=connect_db,
            desc=f"Fetching YFinance {dataset}",
            table_name="yfinance_holders",
            show_progress=show_progress,
            **kwargs,
        )

    @jupyter_safe
    async def get_major_holders(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch major holders summary metrics.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="major_holders",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance major_holders",
            table_name="yfinance_major_holders",
            show_progress=show_progress,
            **kwargs,
        )

    # --- Insider ---

    @jupyter_safe
    async def get_insider_roster_holders(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch insider roster holders.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="insider_roster_holders",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance insider_roster_holders",
            table_name="yfinance_insider_roster_holders",
            show_progress=show_progress,
            **kwargs,
        )

    @jupyter_safe
    async def get_insider_purchases(
        self, *, tickers: list[str], connect_db: str | Path | None = None, show_progress: bool = True, **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch insider purchases.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="insider_purchases",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance insider_purchases",
            table_name="yfinance_insider_purchases",
            show_progress=show_progress,
            **kwargs,
        )

    # --- Calendar ---

    @jupyter_safe
    async def get_calendar(
        self, *, tickers: list[str], connect_db: str | Path | None = None, show_progress: bool = True, **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch earnings calendar.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="calendar",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance calendar",
            table_name="yfinance_calendar",
            show_progress=show_progress,
            **kwargs,
        )

    # --- Analyst Recommendations ---

    @jupyter_safe
    async def get_recommendations(
        self, *, tickers: list[str], connect_db: str | Path | None = None, show_progress: bool = True, **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch analyst recommendations.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="recommendations",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance recommendations",
            table_name="yfinance_recommendations",
            show_progress=show_progress,
            **kwargs,
        )

    # --- news ---

    @jupyter_safe
    async def get_news(
        self, *, tickers: list[str], connect_db: str | Path | None = None, show_progress: bool = True, **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker news.

        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.

        Returns:
            Polars DataFrame in memory or RunResult when persisting.

        Raises:
            InputError: If tickers list is empty or invalid.
            FetchError: If network/API errors occur during data retrieval.
            TransformError: If data normalization fails.
            WriterError: If persistence fails.
        """
        return await self._dispatch_fetch(
            dataset="news",
            tickers=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance news",
            table_name="yfinance_news",
            show_progress=show_progress,
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
        validate_tickers(symbols)
        bytes_per_item = YF_SIZE_MAP.get(dataset, DEFAULT_BYTES_PER_ITEM)
        validate_memory_usage(
            symbols=symbols,
            connect_db=connect_db,
            bytes_per_item=bytes_per_item,
        )

        # Use BaseClient's common infrastructure
        pbar, pbar_updater = self.create_progress_tracker(
            total_items=total_items,
            unit=unit,
            desc=desc,
            show_progress=show_progress,
        )

        result_obj: pl.DataFrame | RunResult = (
            RunResult(provider="yfinance") if connect_db is not None else pl.DataFrame()
        )
        try:
            async with self.managed_writer(connect_db, show_progress=show_progress) as writer:
                router = create_router(
                    "yfinance",
                    api_key=self.api_key or "",
                    config=self._config,
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
                    on_progress=pbar_updater,
                    **{k: v for k, v in kwargs.items() if k not in (RESERVED_PIPELINE_KEYS | {PRICE_BATCH_SIZE_KEY})},
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

    async def _dispatch_fetch(
        self,
        *,
        dataset: YFinanceDataset,
        tickers: list[str],
        connect_db: str | Path | None,
        desc: str,
        table_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        total_items = len(tickers)
        return await self._fetch_per_ticker(
            dataset=dataset,
            symbols=tickers,
            connect_db=connect_db,
            desc=desc,
            table_name=table_name,
            show_progress=show_progress,
            total_items=total_items,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
