from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import RunResult


class YFinanceClient(BaseClient):
    """YFinance client implementation."""

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
        """Fetch price data (Not implemented)."""
        raise NotImplementedError("YFinance support is not yet implemented")

    async def get_tickers(
        self,
        *,
        tickers: list[str] | None = None,
        connect_db: str | Path | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame | RunResult:
        """Fetch ticker metadata (Not implemented)."""
        raise NotImplementedError("YFinance support is not yet implemented")
