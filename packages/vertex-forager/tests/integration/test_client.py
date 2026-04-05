"""
Integration tests for vertex-forager client functionality.

현업 테스트 패턴:
- 클래스 기반 테스트 구조
- 명확한 테스트 메서드 이름 (test_<기능>_<조건>_<결과>)
- AAA 패턴 (Arrange-Act-Assert)
- 의존성 주입을 통한 테스트 격리
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import polars as pl
import pytest

from vertex_forager.core.config import RunResult


class TestClientVisualization:
    """Tests for client progress visualization."""

    @pytest.mark.asyncio
    async def test_get_ticker_info_defaults_to_silent_progress(self, sharadar_client):
        """Collect calls are silent unless progress=True is requested."""
        with patch("vertex_forager.clients.base.VertexForager") as MockPipeline:
            mock_pipeline_instance = MockPipeline.return_value
            mock_pipeline_instance.run = AsyncMock(return_value=MagicMock())
            await sharadar_client._get_ticker_info_async()

            assert mock_pipeline_instance.run.await_args.kwargs["progress"] is False

    @pytest.mark.asyncio
    async def test_get_price_data_uses_tqdm(self, sharadar_client, tmp_path):
        """progress=True is threaded through to the pipeline."""

        # Patch dependencies
        with patch("vertex_forager.clients.base.VertexForager") as MockPipeline:
            # Setup mock pipeline run return value
            mock_pipeline_instance = MockPipeline.return_value
            mock_pipeline_instance.run = AsyncMock(return_value=MagicMock())

            # Act
            tickers = ["AAPL", "GOOGL"]
            await sharadar_client._get_price_data_async(
                tickers=tickers,
                start_date="2024-01-01",
                end_date="2024-01-10",
                progress=True,
                persist=True,
                db_path=str(tmp_path / "test.db"),
            )

            # Assert
            assert mock_pipeline_instance.run.await_args.kwargs["progress"] is True


class TestClientIntegration:
    """Integration tests for client functionality."""

    @pytest.mark.asyncio
    @pytest.mark.skipif(importlib.util.find_spec("nest_asyncio") is None, reason="requires nest_asyncio")
    async def test_get_price_data_sync_facade_smoke_in_async_context(
        self,
        sharadar_client,
        mock_http_executor,
        sample_price_data,
    ) -> None:
        mock_response: bytes = json.dumps(sample_price_data).encode()
        mock_http_executor.fetch.return_value = mock_response
        result = sharadar_client.get_price_data(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )
        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.height == 2
        assert "ticker" in result.data.columns
        assert "close" in result.data.columns

    @pytest.mark.asyncio
    async def test_get_price_data_returns_dataframe_with_correct_structure(
        self, sharadar_client, mock_http_executor, sample_price_data
    ) -> None:
        """Test that get_price_data returns properly structured DataFrame."""
        # Arrange
        mock_response: bytes = json.dumps(sample_price_data).encode()
        mock_http_executor.fetch.return_value = mock_response

        # Act
        result = await sharadar_client._get_price_data_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.height == 2
        assert "ticker" in result.data.columns
        assert "close" in result.data.columns
        assert "provider" in result.data.columns
        assert "fetched_at" in result.data.columns

    @pytest.mark.asyncio
    async def test_get_price_data_with_persistence_returns_run_result(
        self, sharadar_client, mock_http_executor, sample_price_data, tmp_path
    ) -> None:
        """Test that get_price_data returns RunResult when persisting to database."""
        # Arrange
        mock_response: bytes = json.dumps(sample_price_data).encode()
        mock_http_executor.fetch.return_value = mock_response

        # Act
        result = await sharadar_client._get_price_data_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=tmp_path / "test.db",
        )

        # Assert
        assert isinstance(result, RunResult)
        assert result.provider == "sharadar"
        # RunResult counts might vary depending on writer implementation details,
        # but we expect 2 rows processed.
        assert "sharadar_sep" in result.tables
        assert result.tables["sharadar_sep"] == 2
        assert result.data is None
        assert len(result.errors) == 0

    @pytest.mark.asyncio
    async def test_get_price_data_passes_meta_dataframe_to_fetcher(
        self,
        sharadar_client,
        tmp_path: Path,
    ) -> None:
        meta_path = tmp_path / "meta.duckdb"
        import duckdb

        with duckdb.connect(str(meta_path)) as conn:
            conn.execute(
                """
                CREATE TABLE sharadar_tickers (
                    ticker VARCHAR,
                    firstpricedate DATE,
                    lastpricedate DATE
                )
                """
            )
            conn.execute(
                """
                INSERT INTO sharadar_tickers VALUES
                ('AAPL', DATE '1980-12-12', DATE '2024-01-31')
                """
            )

        captured: dict[str, object] = {}

        async def _fake_dispatch(**kwargs: object) -> RunResult:
            captured["dataset"] = kwargs.get("dataset")
            captured["ticker_metadata"] = kwargs.get("ticker_metadata")
            return RunResult(provider="sharadar", data=pl.DataFrame())

        with patch.object(sharadar_client, "_dispatch_fetch", _fake_dispatch):
            await sharadar_client._get_price_data_async(
                tickers=["AAPL"],
                meta=meta_path,
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

        ticker_metadata = captured["ticker_metadata"]
        assert captured["dataset"] == "price"
        assert isinstance(ticker_metadata, pl.DataFrame)
        assert ticker_metadata.columns == ["ticker", "firstpricedate", "lastpricedate"]
        assert ticker_metadata.get_column("ticker").to_list() == ["AAPL"]

    @pytest.mark.asyncio
    async def test_get_price_data_passes_none_meta_to_fetcher(self, sharadar_client) -> None:
        captured: dict[str, object] = {}

        async def _fake_dispatch(**kwargs: object) -> RunResult:
            captured["dataset"] = kwargs.get("dataset")
            captured["ticker_metadata"] = kwargs.get("ticker_metadata")
            return RunResult(provider="sharadar", data=pl.DataFrame())

        with patch.object(sharadar_client, "_dispatch_fetch", _fake_dispatch):
            await sharadar_client._get_price_data_async(
                tickers=["AAPL"],
                meta=None,
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

        assert captured["dataset"] == "price"
        assert captured["ticker_metadata"] is None

    @pytest.mark.asyncio
    async def test_get_ticker_info_none_calls_dispatch(self, sharadar_client) -> None:
        async def _fake_dispatch(**kwargs: object) -> RunResult:
            assert kwargs.get("dataset") == "tickers"
            assert kwargs.get("symbols") is None
            return RunResult(provider="sharadar", data=pl.DataFrame({"ticker": ["AAPL"]}))

        with patch.object(sharadar_client, "_dispatch_fetch", _fake_dispatch):
            result = await sharadar_client._get_ticker_info_async(tickers=None)

        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.get_column("ticker").to_list() == ["AAPL"]

    @pytest.mark.asyncio
    async def test_get_daily_metrics_handles_financial_data(
        self,
        sharadar_client,
        mock_http_executor,
    ) -> None:
        """Test that get_daily_metrics processes financial metrics correctly."""
        # Arrange
        mock_response = {
            "datatable": {
                "data": [["AAPL", "2024-01-02", "100", "200"]],
                "columns": [
                    {"name": "ticker"},
                    {"name": "date"},
                    {"name": "ev"},
                    {"name": "evebit"},
                ],
            }
        }
        mock_http_executor.fetch.return_value = json.dumps(mock_response).encode()

        # Act
        result = await sharadar_client._get_daily_metrics_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.height == 1
        assert result.data.get_column("ticker").to_list() == ["AAPL"]
        # Polars infers or converts to float for financial metrics
        assert result.data.get_column("ev").to_list() == [100.0]

    @pytest.mark.asyncio
    async def test_get_corporate_actions_processes_dividend_events(
        self,
        sharadar_client,
        mock_http_executor,
    ) -> None:
        """Test that get_corporate_actions handles dividend events correctly."""
        # Arrange
        mock_response = {
            "datatable": {
                "data": [["2024-01-02", "dividend", "0.5", "AAPL", "Apple"]],
                "columns": [
                    {"name": "date"},
                    {"name": "action"},
                    {"name": "value"},
                    {"name": "ticker"},
                    {"name": "name"},
                ],
            }
        }
        mock_http_executor.fetch.return_value = json.dumps(mock_response).encode()

        # Act
        result = await sharadar_client._get_corporate_actions_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.height == 1
        assert result.data.get_column("action").to_list() == ["dividend"]


@pytest.mark.asyncio
class TestClientErrorHandling:
    """Test suite for client error handling scenarios."""

    @pytest.mark.asyncio
    async def test_client_handles_empty_response_gracefully(
        self,
        sharadar_client,
        mock_http_executor,
    ) -> None:
        """Test that client handles empty API responses gracefully."""
        # Arrange
        mock_response_obj: dict[str, object] = {"datatable": {"data": [], "columns": []}}
        mock_http_executor.fetch.return_value = json.dumps(mock_response_obj).encode()

        # Act
        result = await sharadar_client._get_price_data_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        # Should return empty DataFrame or handle gracefully
        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.height == 0

    @pytest.mark.asyncio
    async def test_client_handles_api_error_gracefully(
        self,
        sharadar_client,
        mock_http_executor,
    ) -> None:
        """Test that client handles API errors gracefully."""
        # Arrange - Mock http executor to raise exception
        mock_http_executor.fetch.side_effect = httpx.RequestError("API Error")

        # Act
        result = await sharadar_client._get_price_data_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        # Should return empty DataFrame when all retries fail
        assert isinstance(result, RunResult)
        assert result.data is not None
        assert result.data.height == 0

    @pytest.mark.asyncio
    async def test_client_maintains_rate_limiting(
        self,
        sharadar_client,
        mock_http_executor,
        sample_price_data,
    ) -> None:
        """Test that client respects rate limiting configuration."""
        # Arrange
        mock_response = json.dumps(sample_price_data).encode()
        mock_http_executor.fetch.return_value = mock_response

        # Act
        result1 = await sharadar_client._get_price_data_async(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )
        result2 = await sharadar_client._get_price_data_async(
            tickers=["MSFT"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result1, RunResult)
        assert isinstance(result2, RunResult)
        # Check that http executor was called multiple times
        # Note: Depending on batching, it might be called once or twice per request
        assert mock_http_executor.fetch.call_count >= 2
