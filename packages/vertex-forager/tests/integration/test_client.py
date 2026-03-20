"""
Integration tests for vertex-forager client functionality.

현업 테스트 패턴:
- 클래스 기반 테스트 구조
- 명확한 테스트 메서드 이름 (test_<기능>_<조건>_<결과>)
- AAA 패턴 (Arrange-Act-Assert)
- 의존성 주입을 통한 테스트 격리
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import polars as pl
import pytest

from vertex_forager.core.config import RunResult


class TestClientVisualization:
    """Tests for client progress visualization (Spinner/Tqdm)."""

    @pytest.mark.asyncio
    async def test_get_ticker_info_uses_spinner(self, sharadar_client):
        """Test that get_ticker_info (ambiguous task) uses Spinner."""

        # Patch dependencies
        # 1. Pipeline run to avoid actual execution
        # 2. Spinner to verify usage
        with (
            patch("vertex_forager.clients.base.VertexForager") as MockPipeline,
            patch("vertex_forager.providers.sharadar.client.Spinner") as MockSpinner,
        ):
            # Setup mock pipeline run return value
            mock_pipeline_instance = MockPipeline.return_value
            dummy_result = MagicMock()
            mock_pipeline_instance.run = AsyncMock(
                return_value=dummy_result
            )  # Return dummy result

            # Setup mock spinner context manager
            mock_spinner_instance = MockSpinner.return_value
            mock_spinner_instance.__enter__.return_value = mock_spinner_instance

            # Act
            await sharadar_client.get_ticker_info()

            # Assert
            # Verify Spinner was initialized
            MockSpinner.assert_called()
            # Verify Spinner was used as context manager
            mock_spinner_instance.__enter__.assert_called()
            mock_spinner_instance.__exit__.assert_called()

    @pytest.mark.asyncio
    async def test_get_price_data_uses_tqdm(self, sharadar_client, tmp_path):
        """Test that get_price_data (per-ticker task) uses tqdm."""

        # Patch dependencies
        with (
            patch("vertex_forager.clients.base.VertexForager") as MockPipeline,
            patch("vertex_forager.clients.base.tqdm") as MockTqdm,
        ):
            # Setup mock pipeline run return value
            mock_pipeline_instance = MockPipeline.return_value
            mock_pipeline_instance.run = AsyncMock(return_value=MagicMock())

            # Setup mock tqdm context manager
            mock_tqdm_instance = MockTqdm.return_value
            mock_tqdm_instance.__enter__.return_value = mock_tqdm_instance

            # Act
            tickers = ["AAPL", "GOOGL"]
            await sharadar_client.get_price_data(
                tickers=tickers,
                start_date="2024-01-01",
                end_date="2024-01-10",
                persist=True,
                db_path=str(tmp_path / "test.db"),
            )

            # Assert
            # Verify tqdm was initialized
            MockTqdm.assert_called()
            # We expect at least one call related to price data
            # Metadata fetching might trigger another one
            # Check if any call has the correct total
            # Note: total is now number of TICKERS (2), not batches (1).
            calls = MockTqdm.call_args_list
            price_call_found = False
            for call in calls:
                # With 2 tickers, we expect total=2
                if call.kwargs.get("total") == 2:
                    price_call_found = True
                    break
            assert price_call_found

            # Verify tqdm was used
            # Refactored implementation does not use context manager
            # It uses try/finally with close()
            # mock_tqdm_instance.__enter__.assert_called()
            mock_tqdm_instance.close.assert_called()


class TestClientIntegration:
    """Integration tests for client functionality."""

    @pytest.mark.asyncio
    async def test_get_price_data_returns_dataframe_with_correct_structure(
        self, sharadar_client, mock_http_executor, sample_price_data
    ) -> None:
        """Test that get_price_data returns properly structured DataFrame."""
        # Arrange
        mock_response: bytes = json.dumps(sample_price_data).encode()
        mock_http_executor.fetch.return_value = mock_response

        # Act
        result = await sharadar_client.get_price_data(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result, pl.DataFrame)
        assert result.height == 2
        assert "ticker" in result.columns
        assert "close" in result.columns
        assert "provider" in result.columns
        assert "fetched_at" in result.columns

    @pytest.mark.asyncio
    async def test_get_price_data_with_persistence_returns_run_result(
        self, sharadar_client, mock_http_executor, sample_price_data, tmp_path
    ) -> None:
        """Test that get_price_data returns RunResult when persisting to database."""
        # Arrange
        mock_response: bytes = json.dumps(sample_price_data).encode()
        mock_http_executor.fetch.return_value = mock_response

        # Act
        result = await sharadar_client.get_price_data(
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
        assert len(result.errors) == 0

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
        result = await sharadar_client.get_daily_metrics(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result, pl.DataFrame)
        assert result.height == 1
        assert result.get_column("ticker").to_list() == ["AAPL"]
        # Polars infers or converts to float for financial metrics
        assert result.get_column("ev").to_list() == [100.0]

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
        result = await sharadar_client.get_corporate_actions(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result, pl.DataFrame)
        assert result.height == 1
        assert result.get_column("action").to_list() == ["dividend"]


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
        mock_response_obj: dict[str, object] = {
            "datatable": {"data": [], "columns": []}
        }
        mock_http_executor.fetch.return_value = json.dumps(mock_response_obj).encode()

        # Act
        result = await sharadar_client.get_price_data(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        # Should return empty DataFrame or handle gracefully
        assert isinstance(result, pl.DataFrame)
        assert result.height == 0

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
        result = await sharadar_client.get_price_data(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        # Should return empty DataFrame when all retries fail
        assert isinstance(result, pl.DataFrame)
        assert result.height == 0

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
        result1 = await sharadar_client.get_price_data(
            tickers=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )
        result2 = await sharadar_client.get_price_data(
            tickers=["MSFT"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            connect_db=None,
        )

        # Assert
        assert isinstance(result1, pl.DataFrame)
        assert isinstance(result2, pl.DataFrame)
        # Check that http executor was called multiple times
        # Note: Depending on batching, it might be called once or twice per request
        assert mock_http_executor.fetch.call_count >= 2
