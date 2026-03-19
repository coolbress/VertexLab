"""
Integration tests for router factory functionality.
"""

from __future__ import annotations

import pytest
from vertex_forager.core.config import EngineConfig
from vertex_forager.providers.sharadar.router import SharadarRouter
from vertex_forager.providers.yfinance.router import YFinanceRouter
from vertex_forager.routers import create_router


class TestRouterFactory:
    """Tests for the router factory (create_router)."""

    def test_create_sharadar_router_success(self) -> None:
        """Test creating a Sharadar router via create_router."""
        config = EngineConfig(requests_per_minute=100)
        api_key = "test_api_key"

        router = create_router(
            provider="sharadar", api_key=api_key, config=config, start_date="2024-01-01"
        )

        assert isinstance(router, SharadarRouter)
        # Verify internal attributes via public interface
        assert router.api_key == api_key
        assert router.rate_limit == 100

    def test_create_router_invalid_provider(self):
        """Test error when provider is invalid."""
        config = EngineConfig(requests_per_minute=100)

        with pytest.raises(KeyError, match="Unsupported router: invalid_provider"):
            create_router(provider="invalid_provider", api_key="key", config=config)

    def test_create_yfinance_router_success(self) -> None:
        """Verify create_router returns a YFinanceRouter with default rate limit."""
        config = EngineConfig(requests_per_minute=60)
        router = create_router(provider="yfinance", api_key=None, config=config)
        assert isinstance(router, YFinanceRouter)
        assert router.rate_limit == 60
