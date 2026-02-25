"""Unit tests for YFinanceRouter behavior and parsing."""
from __future__ import annotations

import pickle

import pandas as pd
import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, RequestSpec, ParseResult
from vertex_forager.providers.yfinance.router import YFinanceRouter


class TestYFinanceRouterUnit:
    """Unit tests verifying YFinanceRouter job generation and parsing behavior."""

    def test_provider_property(self, yfinance_router: YFinanceRouter) -> None:
        """Verify provider property returns 'yfinance'."""
        assert yfinance_router.provider == "yfinance"

    @pytest.mark.asyncio
    async def test_generate_jobs_requires_symbols_for_non_tickers(
        self, yfinance_router: YFinanceRouter
    ) -> None:
        """Ensure ValueError when symbols are missing for non-tickers datasets."""
        with pytest.raises(ValueError):
            _ = [job async for job in yfinance_router.generate_jobs(dataset="price", symbols=None)]

    @pytest.mark.asyncio
    async def test_generate_jobs_builds_per_symbol_jobs_for_price(
        self, yfinance_router: YFinanceRouter
    ) -> None:
        """Ensure per-symbol jobs are generated for price dataset."""
        symbols = ["AAPL", "MSFT", "TSLA"]
        jobs = [job async for job in yfinance_router.generate_jobs(dataset="price", symbols=symbols)]
        assert len(jobs) == len(symbols)
        for i, sym in enumerate(symbols):
            job = jobs[i]
            assert job.provider == "yfinance"
            assert job.dataset == "price"
            assert job.spec.url == f"yfinance://{sym}"
            assert job.context.get("is_batch") is not True

    def test_parse_returns_frame_for_price_dataset(self, yfinance_router: YFinanceRouter, yf_price_df: pd.DataFrame) -> None:
        """Verify parse returns a populated frame for price payload."""
        payload = pickle.dumps(yf_price_df)
        job = FetchJob(
            provider="yfinance",
            dataset="price",
            symbol="AAPL",
            spec=RequestSpec(url="yfinance://AAPL", params={"dataset": "price"}),
            context={"symbol": "AAPL"},
        )
        result = yfinance_router.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 1
        packet = result.packets[0]
        assert packet.provider == "yfinance"
        assert packet.table == "yfinance_price"
        assert isinstance(packet.frame, pl.DataFrame)
        assert packet.frame.height == 2
        assert {"date", "open", "close", "ticker"}.issubset(set(packet.frame.columns))

    def test_parse_handles_empty_dataframe(self, yfinance_router: YFinanceRouter) -> None:
        """Verify parse returns zero packets for empty DataFrame payload."""
        empty_df = pd.DataFrame(columns=["date", "open", "close", "ticker"])
        payload = pickle.dumps(empty_df)
        job = FetchJob(
            provider="yfinance",
            dataset="price",
            symbol="AAPL",
            spec=RequestSpec(url="yfinance://AAPL", params={"dataset": "price"}),
            context={"symbol": "AAPL"},
        )
        result = yfinance_router.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 0
