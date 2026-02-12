import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from vertex_forager.providers.sharadar.client import SharadarClient

@pytest.fixture
def mock_client():
    client = SharadarClient(api_key="test_key", rate_limit=100)
    client._run = AsyncMock(return_value=MagicMock())  # Mock _run to avoid actual execution
    return client

@pytest.mark.asyncio
async def test_fetch_pagination_show_progress_true(mock_client):
    with patch("vertex_forager.providers.sharadar.client.tqdm") as mock_tqdm, \
         patch("vertex_forager.providers.sharadar.client.create_writer") as mock_create_writer, \
         patch("vertex_forager.providers.sharadar.client.create_router"):
        
        # Setup mocks
        mock_writer = MagicMock()
        mock_writer.__aenter__ = AsyncMock(return_value=mock_writer)
        mock_writer.__aexit__ = AsyncMock(return_value=None)
        mock_create_writer.return_value = mock_writer
        
        await mock_client._fetch_pagination(
            dataset="test",
            desc="test",
            table_name="test",
            connect_db=None,
            show_progress=True
        )
        # Check if tqdm was called with disable=False (or disable=not True -> False)
        # Note: disable=False is default, but we passed disable=not show_progress
        call_kwargs = mock_tqdm.call_args.kwargs
        assert call_kwargs.get("disable") is False

@pytest.mark.asyncio
async def test_fetch_pagination_show_progress_false(mock_client):
    with patch("vertex_forager.providers.sharadar.client.tqdm") as mock_tqdm, \
         patch("vertex_forager.providers.sharadar.client.create_writer") as mock_create_writer, \
         patch("vertex_forager.providers.sharadar.client.create_router"):
        
        # Setup mocks
        mock_writer = MagicMock()
        mock_writer.__aenter__ = AsyncMock(return_value=mock_writer)
        mock_writer.__aexit__ = AsyncMock(return_value=None)
        mock_create_writer.return_value = mock_writer

        await mock_client._fetch_pagination(
            dataset="test",
            desc="test",
            table_name="test",
            connect_db=None,
            show_progress=False
        )
        # Check if tqdm was NOT called (implementation skips it entirely if False)
        mock_tqdm.assert_not_called()
