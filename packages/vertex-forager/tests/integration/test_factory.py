"""
Integration tests for client factory functionality.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from vertex_forager.api import create_client
from vertex_forager.providers.sharadar.client import SharadarClient


class TestClientFactory:
    """Tests for the client factory (create_client)."""

    def test_create_sharadar_client_success(self):
        """Test creating a Sharadar client via create_client."""
        api_key = "test_api_key"
        client = create_client(
            provider="sharadar",
            api_key=api_key,
            rate_limit=100
        )
        
        assert isinstance(client, SharadarClient)
        # Verify internal config using private attributes if necessary, 
        # or check public properties if available. 
        # Here we just check type as that confirms factory worked.

    def test_create_client_with_env_var(self):
        """Test creating client using environment variable for API key."""
        with patch.dict(os.environ, {"SHARADAR_API_KEY": "env_key_123"}):
            client = create_client(
                provider="sharadar",
                rate_limit=100
            )
            assert isinstance(client, SharadarClient)

    def test_create_client_missing_api_key(self):
        """Test error when API key is missing."""
        # Ensure env var is not set
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing api_key"):
                create_client(
                    provider="sharadar",
                    rate_limit=100
                )

    def test_create_client_invalid_provider(self):
        """Test error when provider is invalid."""
        with pytest.raises(NotImplementedError, match="Unsupported client: invalid_provider"):
            create_client(
                provider="invalid_provider",
                api_key="key",
                rate_limit=100
            )
