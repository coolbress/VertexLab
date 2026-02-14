"""
Tests for HTTP executor functionality.

현업 테스트 패턴:
- 네트워크 계층 모의 테스트
- 에러 처리 및 재시도 로직 테스트
- 성능 및 동시성 테스트
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import httpx
from httpx import Response

from vertex_forager.core.config import RequestSpec, HttpMethod, RequestAuth
from vertex_forager.core.http import HttpExecutor


class TestHttpExecutor:
    """Tests for HTTP executor functionality."""

    @pytest.fixture
    def http_executor(self, mock_async_client: AsyncMock) -> HttpExecutor:
        """Create HTTP executor with mock client."""
        return HttpExecutor(client=mock_async_client)

    @pytest.fixture
    def sample_request_spec(self) -> RequestSpec:
        """Create sample request specification."""
        return RequestSpec(
            method=HttpMethod.GET,
            url="https://api.example.com/data",
            params={"ticker": "AAPL"},
            headers={"Authorization": "Bearer token123"},
        )

    @pytest.fixture
    def success_response(self) -> Response:
        """Create successful HTTP response."""
        response = MagicMock(spec=Response)
        response.status_code = 200
        response.content = b'{"data": "success"}'
        response.headers = {"Content-Type": "application/json"}
        return response

    @pytest.fixture
    def error_response(self) -> Response:
        """Create error HTTP response."""
        response = MagicMock(spec=Response)
        response.status_code = 500
        response.content = b'{"error": "internal error"}'
        response.headers = {"Content-Type": "application/json"}

        # Mock raise_for_status to raise HTTPStatusError for error responses
        def raise_for_status() -> None:
            raise httpx.HTTPStatusError(
                "Server error", request=MagicMock(), response=response
            )

        response.raise_for_status = raise_for_status

        return response

    @pytest.mark.asyncio
    async def test_fetch_successful_request(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        sample_request_spec: RequestSpec,
        success_response: Response,
    ) -> None:
        """Test successful HTTP request execution."""
        # Arrange
        mock_async_client.request.return_value = success_response

        # Act
        result = await http_executor.fetch(sample_request_spec)

        # Assert
        assert result == b'{"data": "success"}'
        mock_async_client.request.assert_called_once_with(
            "GET",
            "https://api.example.com/data",
            params={"ticker": "AAPL"},
            headers={"Authorization": "Bearer token123"},
            json=None,
            content=None,
            timeout=30.0,
        )

    @pytest.mark.asyncio
    async def test_fetch_injects_bearer_auth(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        success_response: Response,
    ) -> None:
        """Verify Bearer token injection."""
        mock_async_client.request.return_value = success_response
        spec = RequestSpec(
            url="http://test.com",
            method=HttpMethod.GET,
            auth=RequestAuth(kind="bearer", token="secret_token"),
        )

        await http_executor.fetch(spec)

        kwargs = mock_async_client.request.call_args.kwargs
        assert kwargs["headers"]["Authorization"] == "Bearer secret_token"

    @pytest.mark.asyncio
    async def test_fetch_injects_query_auth(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        success_response: Response,
    ) -> None:
        """Verify Query parameter auth injection."""
        mock_async_client.request.return_value = success_response
        spec = RequestSpec(
            url="http://test.com",
            method=HttpMethod.GET,
            auth=RequestAuth(kind="query", query_param="api_key", token="12345"),
        )

        await http_executor.fetch(spec)

        kwargs = mock_async_client.request.call_args.kwargs
        assert kwargs["params"]["api_key"] == "12345"

    @pytest.mark.asyncio
    async def test_fetch_injects_header_auth(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        success_response: Response,
    ) -> None:
        """Verify Custom Header auth injection."""
        mock_async_client.request.return_value = success_response
        spec = RequestSpec(
            url="http://test.com",
            method=HttpMethod.GET,
            auth=RequestAuth(kind="header", header_name="X-API-KEY", token="secret"),
        )

        await http_executor.fetch(spec)

        kwargs = mock_async_client.request.call_args.kwargs
        assert kwargs["headers"]["X-API-KEY"] == "secret"

    @pytest.mark.asyncio
    async def test_fetch_handles_http_errors(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        sample_request_spec: RequestSpec,
        error_response: Response,
    ) -> None:
        """Test that HTTP errors are properly handled."""
        # Arrange
        mock_async_client.request.return_value = error_response

        # Act & Assert
        with pytest.raises(httpx.HTTPStatusError):
            await http_executor.fetch(sample_request_spec)

    @pytest.mark.asyncio
    async def test_fetch_handles_network_errors(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        sample_request_spec: RequestSpec,
    ) -> None:
        """Test that network errors are properly handled."""
        # Arrange
        mock_async_client.request.side_effect = httpx.RequestError(
            "Network error", request=MagicMock()
        )

        # Act & Assert
        with pytest.raises(httpx.RequestError):
            await http_executor.fetch(sample_request_spec)

    @pytest.mark.asyncio
    async def test_fetch_respects_timeout_configuration(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        sample_request_spec: RequestSpec,
        success_response: Response,
    ) -> None:
        """Test that timeout configuration is respected."""
        # Arrange
        mock_async_client.request.return_value = success_response

        # Create spec with custom timeout
        spec_with_timeout = RequestSpec(
            method=HttpMethod.GET, url="https://api.example.com/data", timeout_s=10.0
        )

        # Act
        await http_executor.fetch(spec_with_timeout)

        # Assert
        mock_async_client.request.assert_called_once_with(
            "GET",
            "https://api.example.com/data",
            params={},
            headers={},
            json=None,
            content=None,
            timeout=10.0,
        )

    @pytest.mark.asyncio
    async def test_fetch_handles_empty_response(
        self,
        http_executor: HttpExecutor,
        mock_async_client: AsyncMock,
        sample_request_spec: RequestSpec,
    ) -> None:
        """Test handling of empty responses."""
        # Arrange
        empty_response = MagicMock(spec=Response)
        empty_response.status_code = 200
        empty_response.content = b""
        empty_response.headers = {}

        mock_async_client.request.return_value = empty_response

        # Act
        result = await http_executor.fetch(sample_request_spec)

        # Assert
        assert result == b""


class TestHttpExecutorConcurrency:
    """Tests for HTTP executor concurrency behavior."""

    @pytest.mark.asyncio
    async def test_executor_handles_concurrent_requests(
        self, mock_async_client: AsyncMock
    ) -> None:
        """Test that executor can handle concurrent requests."""
        # Arrange
        executor = HttpExecutor(client=mock_async_client)

        success_response = MagicMock(spec=Response)
        success_response.status_code = 200
        success_response.content = b'{"data": "success"}'

        mock_async_client.request.return_value = success_response

        spec1 = RequestSpec(url="https://api.example.com/data1", method=HttpMethod.GET)
        spec2 = RequestSpec(url="https://api.example.com/data2", method=HttpMethod.GET)

        # Act - execute requests concurrently
        results = await asyncio.gather(executor.fetch(spec1), executor.fetch(spec2))

        # Assert
        assert len(results) == 2
        assert results[0] == b'{"data": "success"}'
        assert results[1] == b'{"data": "success"}'
        assert mock_async_client.request.call_count == 2
