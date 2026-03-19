from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from vertex_forager.clients import base as base_module
from vertex_forager.core.config import RunResult
from vertex_forager.core.types import SharadarDataset
from vertex_forager.providers.sharadar.client import FetchConfig, SharadarClient


@pytest.fixture
def mock_client() -> SharadarClient:
    """Fixture that returns a SharadarClient with a mocked run_pipeline method."""
    client = SharadarClient(api_key="test_key", rate_limit=100)
    # Mock run_pipeline to avoid actual execution
    # Ensure it returns a MagicMock that acts as a RunResult if needed
    mock_run = AsyncMock()
    mock_run.return_value = MagicMock()
    client.run_pipeline = mock_run  # type: ignore[method-assign]
    client.last_run = RunResult(provider="sharadar")
    return client


@pytest.mark.asyncio
async def test_fetch_pagination_show_progress_true(mock_client):
    with (
        patch.object(base_module, "tqdm") as mock_tqdm,
        patch.object(
            base_module, "create_writer"
        ) as mock_create_writer,
        patch("vertex_forager.providers.sharadar.client.create_router"),
    ):
        # Setup mocks
        mock_writer = MagicMock()
        mock_writer.__aenter__ = AsyncMock(return_value=mock_writer)
        mock_writer.__aexit__ = AsyncMock(return_value=None)
        mock_create_writer.return_value = mock_writer

        cfg = FetchConfig(
            dataset=cast(SharadarDataset, "test"),
            symbols=None,
            connect_db=":memory:",
            desc="test",
            table_name="test",
            show_progress=True,
            total_items=None,
            unit="pages",
            start_date=None,
            end_date=None,
            extra={},
        )
        await mock_client._fetch_pagination(cfg)
        # Check if tqdm was called with disable=False (or disable=not True -> False)
        # Note: disable=False is default, but we passed disable=not show_progress
        assert mock_tqdm.called, "tqdm should have been called"
        call_kwargs = mock_tqdm.call_args.kwargs
        assert call_kwargs.get("disable") is False


@pytest.mark.asyncio
async def test_fetch_pagination_show_progress_false(mock_client):
    with (
        patch.object(base_module, "tqdm") as mock_tqdm,
        patch.object(
            base_module, "create_writer"
        ) as mock_create_writer,
        patch("vertex_forager.providers.sharadar.client.create_router"),
    ):
        # Setup mocks
        mock_writer = MagicMock()
        mock_writer.__aenter__ = AsyncMock(return_value=mock_writer)
        mock_writer.__aexit__ = AsyncMock(return_value=None)
        mock_create_writer.return_value = mock_writer

        cfg = FetchConfig(
            dataset=cast(SharadarDataset, "test"),
            symbols=None,
            connect_db=":memory:",
            desc="test",
            table_name="test",
            show_progress=False,
            total_items=None,
            unit="pages",
            start_date=None,
            end_date=None,
            extra={},
        )
        await mock_client._fetch_pagination(cfg)
        # Check if tqdm was NOT called (implementation skips it entirely if False)
        mock_tqdm.assert_not_called()
