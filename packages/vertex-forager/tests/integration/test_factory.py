"""
Integration tests for client factory functionality.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from vertex_forager.api import create_client


class TestClientFactory:
    """Tests for the client factory (create_client)."""

    def test_create_sharadar_client_success(self) -> None:
        """Test creating a Sharadar client via create_client."""
        api_key = "test_api_key"  # pragma: allowlist secret (test)
        client = create_client(provider="sharadar", api_key=api_key, rate_limit=100)

        assert client.__class__.__name__ == "SharadarClient"
        # Verify internal config using private attributes if necessary,
        # or check public properties if available.
        # Here we just check type as that confirms factory worked.

    def test_create_client_with_env_var(self):
        """Test creating client using environment variable for API key."""
        with patch.dict(
            os.environ,
            {"SHARADAR_API_KEY": "env_key_123"},  # pragma: allowlist secret (test)
        ):
            client = create_client(provider="sharadar", rate_limit=100)
            assert client.__class__.__name__ == "SharadarClient"

    def test_create_client_missing_api_key(self):
        """Test error when API key is missing."""
        # Ensure env var is not set
        with patch.dict(os.environ):
            if "SHARADAR_API_KEY" in os.environ:
                del os.environ["SHARADAR_API_KEY"]
            with pytest.raises(ValueError, match="Missing api_key"):
                create_client(provider="sharadar", rate_limit=100)

    def test_create_client_invalid_provider(self):
        """Test error when provider is invalid."""
        with pytest.raises(KeyError, match="Unsupported client: invalid_provider"):
            create_client(
                provider="invalid_provider",
                api_key="key",  # pragma: allowlist secret (test)
                rate_limit=100,
            )
