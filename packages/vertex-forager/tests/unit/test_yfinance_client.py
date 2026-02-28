"""Unit tests for YFinance client and router behavior."""

from __future__ import annotations

import pytest

from vertex_forager.clients import create_client
from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.providers.yfinance.router import YFinanceRouter
from typing import cast, Any


class TestYFinanceClientDefaults:
    """Verify default configuration and client creation behavior."""
    def test_client_init_defaults(self) -> None:
        client = YFinanceClient()
        assert client.api_key is None
        assert client._config.requests_per_minute == 60

    def test_create_client_without_api_key(self) -> None:
        client = create_client(provider="yfinance", rate_limit=1_000)
        assert isinstance(client, YFinanceClient)
        assert client.api_key is None
        # It allows higher rate limits but warns
        assert client._config.requests_per_minute == 1_000
    
    def test_create_client_ignores_user_api_key(self) -> None:
        client = create_client(provider="yfinance", api_key="user_supplied", rate_limit=5)
        assert isinstance(client, YFinanceClient)
        assert client.api_key is None
        assert client._config.requests_per_minute == 5


class TestYFinanceRouterDateParams:
    """Verify date-parameter behavior for YFinanceRouter price jobs."""
    @pytest.mark.asyncio
    async def test_price_jobs_without_dates_omit_start_end(self) -> None:
        router = YFinanceRouter(rate_limit=60, start_date=None, end_date=None)
        jobs = [job async for job in router.generate_jobs(dataset="price", symbols=["AAPL", "MSFT"])]
        assert len(jobs) == 2
        for job in jobs:
            params = job.spec.params
            params_dict = cast("dict[str, Any]", params)
            lib = cast("dict[str, Any]", params_dict.get("lib", {}))
            lib_kwargs = cast("dict[str, Any]", lib.get("kwargs", {}))
            assert "start" not in lib_kwargs
            assert "end" not in lib_kwargs
    
    @pytest.mark.asyncio
    async def test_generate_jobs_invalid_symbols_raises(self) -> None:
        router = YFinanceRouter(rate_limit=60, start_date=None, end_date=None)
        with pytest.raises(ValueError):
            _ = [job async for job in router.generate_jobs(dataset="price", symbols=["", "   ", "@@"])]
