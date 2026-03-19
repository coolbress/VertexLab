"""Unit tests for YFinanceRouter behavior and parsing."""
from __future__ import annotations

import io
import json
import pickle

import pandas as pd
import polars as pl
import pytest
from vertex_forager.core.config import FetchJob, ParseResult, RequestSpec
from vertex_forager.exceptions import TransformError
from vertex_forager.providers.yfinance.router import YFinanceRouter


class TestYFinanceRouterUnit:
    """Unit tests verifying YFinanceRouter job generation and parsing behavior."""

    def make_fetch_job(self, dataset: str = "price", symbol: str = "AAPL") -> FetchJob:
        return FetchJob(
            provider="yfinance",
            dataset=dataset,
            symbol=symbol,
            spec=RequestSpec(url=f"yfinance://{symbol}", params={"dataset": dataset}),
            context={"symbol": symbol},
        )

    def test_provider_property(self, yfinance_router: YFinanceRouter) -> None:
        """Verify provider property returns 'yfinance'."""
        assert yfinance_router.provider == "yfinance"

    @pytest.mark.asyncio
    async def test_generate_jobs_requires_symbols_for_non_tickers(
        self, yfinance_router: YFinanceRouter
    ) -> None:
        """Ensure ValueError when symbols are missing for non-tickers datasets."""
        with pytest.raises(ValueError, match=r".*"):
            _ = [
                job
                async for job in yfinance_router.generate_jobs(
                    dataset="price", symbols=None
                )
            ]

    @pytest.mark.asyncio
    async def test_generate_jobs_builds_per_symbol_jobs_for_price(
        self, yfinance_router: YFinanceRouter
    ) -> None:
        """Ensure per-symbol jobs are generated for price dataset."""
        symbols = ["AAPL", "MSFT", "TSLA"]
        jobs = [
            job
            async for job in yfinance_router.generate_jobs(
                dataset="price", symbols=symbols
            )
        ]
        assert len(jobs) == len(symbols)
        for i, sym in enumerate(symbols):
            job = jobs[i]
            assert job.provider == "yfinance"
            assert job.dataset == "price"
            assert job.spec.url == f"yfinance://{sym}"
            assert job.context.get("is_batch") is not True

    def test_parse_returns_frame_for_price_dataset(
        self,
        yfinance_router_allow_pickle: YFinanceRouter,
        yf_price_df: pd.DataFrame,
    ) -> None:
        """Verify parse returns a populated frame for price payload."""
        payload = pickle.dumps(yf_price_df)
        job = self.make_fetch_job(dataset="price", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 1
        packet = result.packets[0]
        assert packet.provider == "yfinance"
        assert packet.table == "yfinance_price"
        assert isinstance(packet.frame, pl.DataFrame)
        assert packet.frame.height == 2
        assert {"date", "open", "close", "ticker"}.issubset(set(packet.frame.columns))

    def test_parse_handles_empty_dataframe(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        """Verify parse returns zero packets for empty DataFrame payload."""
        empty_df = pd.DataFrame(columns=["date", "open", "close", "ticker"])
        payload = pickle.dumps(empty_df)
        job = self.make_fetch_job(dataset="price", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 0

    def test_transform_financials_melts_and_normalizes_date(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        df = pd.DataFrame(
            {
                "breakdown": ["Revenue", "NetIncome"],
                "2024-01-01 00:00:00": [1, 2],
                "2024-02-01": [3, 4],
            }
        )
        payload = pickle.dumps(df)
        job = self.make_fetch_job(dataset="financials", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 1
        frame = result.packets[0].frame
        assert "date" in frame.columns
        assert "metric" in frame.columns
        dates = frame.get_column("date").cast(pl.Utf8, strict=False).to_list()
        assert all((" " not in (d or "")) and ("T" not in (d or "")) for d in dates)

    def test_transform_news_defensive_paths(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        good = [
            {
                "id": 1,
                "content": {
                    "title": "T",
                    "provider": {"displayName": "P"},
                    "contentType": "story",
                    "canonicalUrl": {"url": "u"},
                    "pubDate": "2025-01-05T09:23:45Z",
                },
            }
        ]
        bad = [{"id": 2, "content": {"title": "X"}}]
        payload = pickle.dumps(good + bad)
        job = self.make_fetch_job(dataset="news", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        frame = result.packets[0].frame
        assert {"title", "publisher", "type", "link", "published_at"}.issubset(
            set(frame.columns)
        )
        assert frame.height == 2

    def test_transform_calendar_list_to_first(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        payload = pickle.dumps([{"earnings_date": ["2024-01-01", "2024-01-02"]}])
        job = self.make_fetch_job(dataset="calendar", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        frame = result.packets[0].frame
        assert "earnings_date" in frame.columns
        assert frame.get_column("earnings_date").to_list()[0] == "2024-01-01"

    def test_parse_invalid_payload_raises(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        job = self.make_fetch_job(dataset="price", symbol="AAPL")
        with pytest.raises(pickle.UnpicklingError):
            _ = yfinance_router_allow_pickle.parse(job=job, payload=b"not a pickle")

    def test_parse_invalid_payload_raises_when_pickle_not_allowed(
        self, yfinance_router: YFinanceRouter
    ) -> None:
        """When pickle compat is disabled, invalid payload should raise ValueError."""
        job = self.make_fetch_job(dataset="price", symbol="AAPL")
        with pytest.raises(TransformError):
            _ = yfinance_router.parse(job=job, payload=b"not a pickle")

    def test_transform_insider_purchases_normalizes_columns(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        """Verify insider_purchases transform maps and filters columns correctly."""
        df = pd.DataFrame({
            "Insider Purchases (Last 6 months)": ["Purchases", None, "Sales"],
            "other_col": [1, 2, 3],
        })
        payload = pickle.dumps(df)
        job = self.make_fetch_job(dataset="insider_purchases", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        frame = result.packets[0].frame
        assert "insider_purchases_last_6m" in frame.columns
        assert frame.height == 2
        assert frame.get_column("insider_purchases_last_6m").to_list() == [
            "Purchases",
            "Sales",
        ]
        assert frame.get_column("other_col").to_list() == [1, 3]

    def test_transform_recommendations_includes_period(
        self, yfinance_router_allow_pickle: YFinanceRouter
    ) -> None:
        """Verify recommendations transform includes period column."""
        df = pd.DataFrame({
            "period": ["0m", "-1m", "-2m"],
            "strongBuy": [5, 3, 2],
            "buy": [10, 8, 6],
        })
        payload = pickle.dumps(df)
        job = self.make_fetch_job(dataset="recommendations", symbol="AAPL")
        result = yfinance_router_allow_pickle.parse(job=job, payload=payload)
        frame = result.packets[0].frame
        assert "period" in frame.columns
        assert frame.height == 3
        assert frame.get_column("period").to_list() == ["0m", "-1m", "-2m"]
        assert "strongbuy" in frame.columns
        assert frame.get_column("strongbuy").to_list() == [5, 3, 2]

    def test_parse_price_ipc_with_secure_router(
        self, yfinance_router: YFinanceRouter, yf_price_df: pd.DataFrame
    ) -> None:
        """Decodes IPC-prefixed payload for price dataset."""
        df_pl = pl.from_pandas(yf_price_df)
        buf = io.BytesIO()
        df_pl.write_ipc(buf)
        payload = b"IPC:" + buf.getvalue()
        job = self.make_fetch_job(dataset="price", symbol="AAPL")
        result = yfinance_router.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 1
        packet = result.packets[0]
        assert packet.provider == "yfinance"
        assert packet.table == "yfinance_price"
        assert isinstance(packet.frame, pl.DataFrame)
        assert packet.frame.height == 2
        assert {"date", "open", "close", "ticker"}.issubset(set(packet.frame.columns))

    def test_parse_news_json_with_secure_router(
        self, yfinance_router: YFinanceRouter
    ) -> None:
        """Decodes JSON-prefixed payload for news dataset."""
        good = [
            {
                "id": 1,
                "content": {
                    "title": "T",
                    "provider": {"displayName": "P"},
                    "contentType": "story",
                    "canonicalUrl": {"url": "u"},
                    "pubDate": "2025-01-05T09:23:45Z",
                },
            }
        ]
        bad = [{"id": 2, "content": {"title": "X"}}]
        payload = b"JSON:" + json.dumps(good + bad).encode("utf-8")
        job = self.make_fetch_job(dataset="news", symbol="AAPL")
        result = yfinance_router.parse(job=job, payload=payload)
        assert isinstance(result, ParseResult)
        frame = result.packets[0].frame
        assert {"title", "publisher", "type", "link", "published_at"}.issubset(
            set(frame.columns)
        )
        assert frame.height == 2
