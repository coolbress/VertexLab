"""
Test fixtures and utilities for vertex-forager tests.

현업에서 사용하는 테스트 패턴:
- pytest fixtures를 활용한 의존성 주입
- 클래스 기반 테스트 구조
- 명확한 테스트 계층 구조
- 모의 객체(Mock)를 통한 외부 의존성 제거
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
import importlib
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

from httpx import AsyncClient
import pandas as pd
import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, FramePacket, RequestSpec
from vertex_forager.core.http import HttpExecutor
from vertex_forager.providers.sharadar.router import SharadarRouter
from vertex_forager.providers.yfinance.router import YFinanceRouter

if TYPE_CHECKING:
    # Type-only import for annotations; avoid runtime import to satisfy Ruff TC001
    from vertex_forager.providers.sharadar.client import SharadarClient


@pytest.fixture
def mock_async_client() -> AsyncMock:
    """Create a mock AsyncClient with run_async.

    Returns:
        AsyncMock: AsyncMock configured with spec=AsyncClient. It simulates an
        HTTPX AsyncClient and provides a run_async coroutine used by the HTTP
        executor. The added run_async method allows tests to await network-like
        behavior without performing real I/O.
    """
    mock = AsyncMock(spec=AsyncClient)
    # Add run_async method that HTTP executor expects
    mock.run_async = AsyncMock()
    return mock


@pytest.fixture
def mock_http_executor(mock_async_client: AsyncMock) -> HttpExecutor:
    """Mock HTTP executor with async client."""
    executor = HttpExecutor(client=mock_async_client)
    executor.fetch = AsyncMock()  # type: ignore[method-assign]
    return executor


@pytest.fixture
def sharadar_client_config() -> dict[str, Any]:
    """Sharadar client configuration."""
    return {
        "provider": "sharadar",
        "api_key": "test_api_key_123",
        "rate_limit": 500,
        "base_url": "https://api.sharadar.com",
    }


@pytest.fixture
def sharadar_client(
    sharadar_client_config: dict[str, Any],
    mock_http_executor: HttpExecutor,
    mock_async_client: AsyncMock,
) -> Generator[SharadarClient, None, None]:
    """Sharadar client instance with mocked HttpExecutor."""
    from unittest.mock import patch

    # Define a no-op decorator to bypass jupyter_safe
    def no_op_decorator(func):
        return func

    # Patch jupyter_safe in the utils module where it is defined
    with patch("vertex_forager.utils.jupyter_safe", side_effect=no_op_decorator):
        # Reload the client module to apply the patched decorator
        import vertex_forager.providers.sharadar.client as client_module

        importlib.reload(client_module)

        from vertex_forager.providers.sharadar.client import SharadarClient

        # Patch HttpExecutor in the base client module (where _run is defined)
        # Ensure that when the client creates an HttpExecutor, it gets our mock
        with patch("vertex_forager.clients.base.HttpExecutor") as MockHttpExecutorClass:
            MockHttpExecutorClass.return_value = mock_http_executor

            # Also patch default_async_client in base client
            # to prevent actual network connections
            with patch(
                "vertex_forager.clients.base.default_async_client"
            ) as mock_default_client:
                mock_default_client.return_value = mock_async_client
                # Ensure mock_async_client supports async context manager protocol
                mock_async_client.__aenter__.return_value = mock_async_client
                mock_async_client.__aexit__.return_value = None

                # Create real client
                client = SharadarClient(
                    api_key=sharadar_client_config["api_key"],
                    rate_limit=sharadar_client_config["rate_limit"],
                    base_url=sharadar_client_config["base_url"],
                )

                yield client


@pytest.fixture
def sharadar_router() -> SharadarRouter:
    """Sharadar router instance."""
    return SharadarRouter(
        api_key="test_api_key_123",
        rate_limit=500,
    )


@pytest.fixture
def sample_price_data() -> dict[str, Any]:
    """Sample price data payload."""
    return {
        "datatable": {
            "data": [
                [
                    "AAPL",
                    "2024-01-02",
                    "100.0",
                    "101.0",
                    "99.5",
                    "100.5",
                    "123",
                    "100.5",
                    "100.5",
                ],
                [
                    "AAPL",
                    "2024-01-03",
                    "101.0",
                    "102.0",
                    "100.0",
                    "101.5",
                    "456",
                    "101.5",
                    "101.5",
                ],
            ],
            "columns": [
                {"name": "ticker"},
                {"name": "date"},
                {"name": "open"},
                {"name": "high"},
                {"name": "low"},
                {"name": "close"},
                {"name": "volume"},
                {"name": "closeadj"},
                {"name": "closeunadj"},
            ],
        }
    }


@pytest.fixture
def sample_ticker_data() -> dict[str, Any]:
    """Sample ticker data payload."""
    return {
        "datatable": {
            "data": [
                ["AAPL", "Apple Inc.", "2024-01-01", "NASDAQ"],
                ["MSFT", "Microsoft Corporation", "2024-01-01", "NASDAQ"],
            ],
            "columns": [
                {"name": "ticker"},
                {"name": "name"},
                {"name": "lastupdated"},
                {"name": "exchange"},
            ],
        }
    }


def create_mock_response(payload: dict[str, Any]) -> bytes:
    """Create mock HTTP response."""
    return json.dumps(payload).encode("utf-8")


def assert_dataframe_structure(df: pl.DataFrame, expected_columns: set[str]) -> None:
    """Assert DataFrame has expected structure."""
    assert isinstance(df, pl.DataFrame)
    assert "provider" in df.columns
    assert "fetched_at" in df.columns
    assert set(df.columns) >= expected_columns


def create_test_fetch_job() -> FetchJob:
    """Create test FetchJob."""
    return FetchJob(
        provider="sharadar",
        dataset="price",
        symbol="AAPL",
        spec=RequestSpec(
            url="https://api.sharadar.com/SEP.json",
            params={"ticker": "AAPL"},
        ),
    )

@pytest.fixture
def yfinance_router() -> YFinanceRouter:
    """Create a YFinanceRouter with explicit rate limiting.

    The rate_limit argument represents requests per minute. For example,
    rate_limit=500 allows up to 500 requests per minute across jobs.

    Returns:
        YFinanceRouter: Router instance configured with rate_limit=500 (requests/min).
    """
    return YFinanceRouter(rate_limit=500, allow_pickle_compat=False)

@pytest.fixture
def yfinance_router_allow_pickle() -> YFinanceRouter:
    """Create a YFinanceRouter with legacy pickle compatibility enabled (unsafe).

    This fixture is only for tests that require pickled payloads.
    """
    return YFinanceRouter(rate_limit=500, allow_pickle_compat=True)

@pytest.fixture
def yf_price_df() -> pd.DataFrame:
    """Provide a sample price DataFrame for tests.

    Returns:
        pandas.DataFrame: Columns include date, open, high, low, close, volume, ticker.
    """
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.0],
            "close": [100.5, 101.5],
            "volume": [123.0, 456.0],
            "ticker": ["AAPL", "AAPL"],
        }
    )


def create_test_frame_packet() -> FramePacket:
    """Create test FramePacket."""
    frame = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": ["2024-01-02"],
            "open": [100.0],
            "close": [101.0],
        }
    )

    return FramePacket(
        provider="sharadar",
        table="sharadar_sep",
        frame=frame,
        observed_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

@pytest.fixture
def pkt_factory():
    """Factory fixture to build a FramePacket for a given table and DataFrame."""
    def _make(table: str, df: pl.DataFrame) -> FramePacket:
        return FramePacket(
            provider="test",
            table=table,
            frame=df,
            observed_at=datetime.now(tz=timezone.utc),
        )
    return _make
