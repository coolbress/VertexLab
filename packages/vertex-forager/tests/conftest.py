"""
Test fixtures and utilities for vertex-forager tests.

현업에서 사용하는 테스트 패턴:
- pytest fixtures를 활용한 의존성 주입
- 클래스 기반 테스트 구조
- 명확한 테스트 계층 구조
- 모의 객체(Mock)를 통한 외부 의존성 제거
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import polars as pl
import pytest
from httpx import AsyncClient

from vertex_forager.core.config import FetchJob, FramePacket, RequestSpec
from vertex_forager.core.http import HttpExecutor
from vertex_forager.providers.sharadar.router import SharadarRouter


@pytest.fixture
def mock_async_client() -> AsyncMock:
    """Mock AsyncClient fixture."""
    return AsyncMock(spec=AsyncClient)


@pytest.fixture
def mock_http_executor(mock_async_client: AsyncMock) -> HttpExecutor:
    """Mock HTTP executor with async client."""
    executor = HttpExecutor(client=mock_async_client)
    executor.fetch = AsyncMock()
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
    mock_async_client: AsyncMock
) -> Any:
    """Sharadar client instance with mocked HttpExecutor."""
    from unittest.mock import patch
    from vertex_forager.providers.sharadar.client import SharadarClient

    # Create real client
    client = SharadarClient(
        api_key=sharadar_client_config["api_key"],
        rate_limit=sharadar_client_config["rate_limit"],
        base_url=sharadar_client_config["base_url"]
    )
    
    # Patch HttpExecutor in the base client module (where _run is defined)
    # This ensures that when the client creates an HttpExecutor, it gets our mock
    with patch("vertex_forager.clients.base.HttpExecutor") as MockHttpExecutorClass:
        MockHttpExecutorClass.return_value = mock_http_executor
        
        # Also patch default_async_client in base client to prevent actual network connections
        with patch("vertex_forager.clients.base.default_async_client") as mock_default_client:
            mock_default_client.return_value = mock_async_client
            # Ensure mock_async_client supports async context manager protocol
            mock_async_client.__aenter__.return_value = mock_async_client
            mock_async_client.__aexit__.return_value = None
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
                ["AAPL", "2024-01-02", "100.0", "101.0", "99.5", "100.5", "123", "100.5", "100.5"],
                ["AAPL", "2024-01-03", "101.0", "102.0", "100.0", "101.5", "456", "101.5", "101.5"],
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


def create_test_frame_packet() -> FramePacket:
    """Create test FramePacket."""
    frame = pl.DataFrame({
        "ticker": ["AAPL"],
        "date": ["2024-01-02"],
        "open": [100.0],
        "close": [101.0],
    })
    
    return FramePacket(
        provider="sharadar",
        table="sharadar_sep",
        frame=frame,
        observed_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
