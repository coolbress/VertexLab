from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import RunResult


class YFinanceClient(BaseClient):
    """YFinance client implementation.

    Provides access to Yahoo Finance data via the unofficial API.

    Characteristics:
        - Free API: No authentication required.
        - Rate Limiting: Default is 120 requests per minute to avoid IP bans.
        - Data Quality: Real-time data may be delayed; historical data accuracy is best-effort.
        - API Key: The `api_key` field is present for interface compatibility but is unused.

    Note:
        `get_price_data` and `get_tickers` are currently stubs and will raise NotImplementedError.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limit: int = 120,
        **kwargs: Any,
    ) -> None:
        """Initialize the YFinance client.

        Args:
            api_key: Not used for YFinance but kept for interface compatibility.
            rate_limit: Requests per minute (default: 120).
            **kwargs: Additional configuration parameters.
        """
        super().__init__(
            api_key=api_key,
            rate_limit=rate_limit,
            **kwargs,
        )

    async def get_price_data(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """
        Fetch price data (Not implemented).

        Args:
            tickers: List of ticker symbols to fetch.
            connect_db: Database connection string or path.
            start_date: Start date for data fetch (YYYY-MM-DD).
            end_date: End date for data fetch (YYYY-MM-DD).
            **kwargs: Additional provider-specific arguments.

        Returns:
            pl.DataFrame | RunResult: Result of the data fetch operation.

        Raises:
            NotImplementedError: Always raised as this method is a stub.
        """
        raise NotImplementedError("YFinance support is not yet implemented")

    async def get_tickers(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """
        Fetch ticker metadata (Not implemented).

        Args:
            tickers: Optional list of tickers to filter.
            connect_db: Database connection string or path.
            **kwargs: Additional provider-specific arguments.

        Returns:
            pl.DataFrame | RunResult: Ticker metadata.

        Raises:
            NotImplementedError: Always raised as this method is a stub.
        """
        raise NotImplementedError("YFinance support is not yet implemented")
