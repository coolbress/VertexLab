from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import polars as pl

from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import RunResult
from vertex_forager.routers import create_router
from vertex_forager.utils import jupyter_safe, validate_memory_usage
from vertex_forager.schema.mapper import SchemaMapper


class YFinanceClient(BaseClient):
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
        rate_limit: int = 60,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            api_key=None,
            rate_limit=60,
            **kwargs,
        )
        self._mapper = SchemaMapper()
        
        
    # ----------------------------------------------------------------
    # Public User Methods
    # ----------------------------------------------------------------
    
    # --- Reference Data ---
    
    @jupyter_safe
    async def get_info(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker metadata/info.
        
        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string/path for persistence.
            **kwargs: Additional provider-specific options.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Raises:
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="info",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance info",
            table_name="yfinance_info",
            show_progress=True,
            total_items=len(tickers),
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
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch historical price data (OHLCV).
        
        Args:
            tickers: List of ticker symbols.
            connect_db: Database connection string.
            start_date: Start date (YYYY-MM-DD). If None, fetches all available history.
            end_date: End date (YYYY-MM-DD). If None, fetches up to latest available date.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Notes:
            Date filters (start/end) apply to price only; other datasets ignore date filters.
        """
        return await self._fetch_per_ticker(
            dataset="price",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance price data",
            table_name="yfinance_price",
            show_progress=True,
            total_items=len(tickers),
            start_date=start_date,
            end_date=end_date,
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
        **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch financial statements (Unified Method).
        
        Args:
            kind: Type of financial statement ('balance_sheet', 'income_stmt', 'cashflow', 'earnings').
            period: Reporting period ('annual' or 'quarterly').
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Notes:
            - yfinance provides only recent periods for financials (e.g., last few years/quarters).
            - Date filters (start/end) are not applicable to financials/earnings endpoints.
            - Financial endpoints do not accept date parameters; start/end/period are ignored. Only a fixed recent set of periods is available (no full-history).
            - For full historical coverage, prefer institutional sources like Sharadar SF1 (ARY/ARQ).
        """
        # Map 'income_stmt' to 'financials' if needed, or keep consistent with yfinance
        # yfinance uses 'financials' property for income statement.
        # But we can support 'income_stmt' alias for clarity.
        target_kind = "financials" if kind == "income_stmt" else kind
        
        if period == "quarterly":
            dataset = f"quarterly_{target_kind}"
        else:
            dataset = target_kind
        return await self._fetch_per_ticker(
            dataset=dataset,
            symbols=tickers,
            connect_db=connect_db,
            desc=f"Fetching YFinance {dataset}",
            table_name="yfinance_financials",
            show_progress=True,
            total_items=len(tickers),
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
        **kwargs: Any
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
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset=kind,
            symbols=tickers,
            connect_db=connect_db,
            desc=f"Fetching YFinance {kind}",
            table_name=f"yfinance_{kind}",
            show_progress=True,
            total_items=len(tickers),
            start_date=start_date,
            end_date=end_date,
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
        **kwargs: Any
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
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        dataset = f"{kind}_holders"
        return await self._fetch_per_ticker(
            dataset=dataset,
            symbols=tickers,
            connect_db=connect_db,
            desc=f"Fetching YFinance {dataset}",
            table_name="yfinance_holders",
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )
    
    @jupyter_safe
    async def get_major_holders(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
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
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="major_holders",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance major_holders",
            table_name="yfinance_major_holders",
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )
    
    # --- Insider ---
    
    @jupyter_safe
    async def get_insider_roster_holders(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
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
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="insider_roster_holders",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance insider_roster_holders",
            table_name="yfinance_insider_roster_holders",
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )
    
    @jupyter_safe
    async def get_insider_purchases(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch insider purchases.
        
        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Raises:
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="insider_purchases",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance insider_purchases",
            table_name="yfinance_insider_purchases",
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )

    # --- Calendar ---
    
    @jupyter_safe
    async def get_calendar(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch earnings calendar.
        
        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Raises:
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="calendar",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance calendar",
            table_name="yfinance_calendar",
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )

    # --- Analyst Recommendations ---
    
    @jupyter_safe
    async def get_recommendations(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch analyst recommendations.
        
        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Raises:
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="recommendations",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance recommendations",
            table_name="yfinance_recommendations",
            show_progress=True,
            total_items=len(tickers),
            **kwargs,
        )

    # --- news ---
    
    @jupyter_safe
    async def get_news(
        self,
        *,
        tickers: list[str],
        connect_db: str | Path | None = None,
        **kwargs: Any
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker news.
        
        Args:
            tickers: List of ticker symbols.
            connect_db: Optional DuckDB connection string.
            **kwargs: Additional provider-specific options.
        
        Returns:
            Polars DataFrame in memory or RunResult when persisting.
        
        Raises:
            ValueError: If tickers list is empty.
            RuntimeError: If pipeline execution fails.
        """
        return await self._fetch_per_ticker(
            dataset="news",
            symbols=tickers,
            connect_db=connect_db,
            desc="Fetching YFinance news",
            table_name="yfinance_news",
            show_progress=True,
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
        common infrastructure while maintaining YFinance-specific logic for
        memory validation and rate limiting optimization.
        
        YFinance-specific characteristics:
        - Free API with no authentication required
        - Conservative rate limiting to avoid IP bans (60 req/min default)
        - Compact data format (smaller memory footprint than premium APIs)
        - Limited ticker universe compared to institutional providers
        
        Args:
            dataset: Dataset name (e.g., "price", "financials", "info")
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
        bytes_per_item = 30 * 1024 if dataset == "info" else 500 * 1024
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
        finally:
            if pbar is not None:
                pbar.close()
        return result_obj
