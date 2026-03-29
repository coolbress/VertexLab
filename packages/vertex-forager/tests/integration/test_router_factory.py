"""
Integration tests for router factory functionality.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from vertex_forager.providers.sharadar.router import SharadarRouter
from vertex_forager.routers import create_router


class TestRouterFactory:
    """Tests for the router factory (create_router)."""

    def test_create_sharadar_router_success(self) -> None:
        """Test creating a Sharadar router via create_router."""
        api_key = "test_api_key"  # pragma: allowlist secret (test)

        router = create_router(
            provider="sharadar",
            api_key=api_key,  # pragma: allowlist secret (test)
            rate_limit=100,
            start_date="2024-01-01",
        )

        assert isinstance(router, SharadarRouter)
        # Verify internal attributes via public interface
        assert router.api_key == api_key
        assert router.rate_limit == 100

    def test_create_router_invalid_provider(self):
        """Test error when provider is invalid."""
        with pytest.raises(KeyError, match="Unsupported router: invalid_provider"):
            create_router(
                provider="invalid_provider",
                api_key="key",  # pragma: allowlist secret (test)
                rate_limit=100,
            )

    def test_create_yfinance_router_success(self) -> None:
        """Verify create_router returns a YFinanceRouter with default rate limit."""
        pytest.importorskip("pandas")
        router = create_router(provider="yfinance", api_key=None, rate_limit=60)
        assert router.__class__.__name__ == "YFinanceRouter"
        assert router.rate_limit == 60

    def test_create_router_accepts_deprecated_config_bridge(self) -> None:
        api_key = "test_api_key"  # pragma: allowlist secret (test)

        with pytest.deprecated_call(match="create_router\\(\\.\\.\\., config=\\.\\.\\.\\) is deprecated"):
            router = create_router(
                provider="sharadar",
                api_key=api_key,  # pragma: allowlist secret (test)
                config=SimpleNamespace(requests_per_minute=100),
            )

        assert isinstance(router, SharadarRouter)
        assert router.rate_limit == 100
