from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
from httpx import AsyncClient, Response
import pytest

from vertex_forager.core.config import HTTPConfig, HttpMethod, RequestAuth, RequestSpec
from vertex_forager.core.http import HttpExecutor

try:
    import pandas as pd
except ImportError:
    pd = None


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
    mock.run_async = AsyncMock()
    return mock


@pytest.fixture
def http_executor(mock_async_client: AsyncMock, http_limits: HTTPConfig | None = None) -> HttpExecutor:
    if http_limits is not None:
        mock_async_client._http_limits = http_limits
    elif not hasattr(mock_async_client, "_http_limits"):
        mock_async_client._http_limits = HTTPConfig()
    return HttpExecutor(client=mock_async_client)


@pytest.fixture
def http_limits() -> HTTPConfig:
    return HTTPConfig()


@pytest.fixture
def sample_request_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url="https://api.example.com/data",
        params={"ticker": "AAPL"},
        headers={"Authorization": "Bearer token123"},
    )


@pytest.fixture
def success_response() -> Response:
    response = MagicMock(spec=Response)
    response.status_code = 200
    response.content = b'{"data": "success"}'
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def error_response() -> Response:
    response = MagicMock(spec=Response)
    response.status_code = 500
    response.content = b'{"error": "internal error"}'
    response.headers = {"Content-Type": "application/json"}

    def raise_for_status() -> None:
        raise httpx.HTTPStatusError("Server error", request=MagicMock(), response=response)

    response.raise_for_status = raise_for_status
    return response


@pytest.mark.asyncio
async def test_fetch_successful_request(
    http_executor: HttpExecutor,
    mock_async_client: AsyncMock,
    sample_request_spec: RequestSpec,
    success_response: Response,
) -> None:
    mock_async_client.run_async.return_value = success_response
    result = await http_executor.fetch(sample_request_spec)
    assert result == b'{"data": "success"}'
    mock_async_client.run_async.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_injects_bearer_query_header_auth(
    http_executor: HttpExecutor,
    mock_async_client: AsyncMock,
    success_response: Response,
) -> None:
    mock_async_client.run_async.return_value = success_response
    token = "".join(["t", "o", "k"])
    await http_executor.fetch(
        RequestSpec(url="http://t", method=HttpMethod.GET, auth=RequestAuth(kind="bearer", token=token))
    )
    assert mock_async_client.run_async.call_args.kwargs["headers"]["Authorization"] == "Bearer " + token
    mock_async_client.run_async.reset_mock()
    await http_executor.fetch(
        RequestSpec(
            url="http://t",
            method=HttpMethod.GET,
            auth=RequestAuth(kind="query", query_param="api_key", token=token),
        )
    )
    assert mock_async_client.run_async.call_args.kwargs["params"]["api_key"] == token
    mock_async_client.run_async.reset_mock()
    await http_executor.fetch(
        RequestSpec(
            url="http://t",
            method=HttpMethod.GET,
            auth=RequestAuth(kind="header", header_name="X-API-KEY", token=token),
        )
    )
    assert mock_async_client.run_async.call_args.kwargs["headers"]["X-API-KEY"] == token


@pytest.mark.asyncio
async def test_fetch_handles_http_and_network_errors(
    http_executor: HttpExecutor,
    mock_async_client: AsyncMock,
    sample_request_spec: RequestSpec,
    error_response: Response,
) -> None:
    mock_async_client.run_async.return_value = error_response
    with pytest.raises(httpx.HTTPStatusError):
        await http_executor.fetch(sample_request_spec)
    mock_async_client.run_async.side_effect = httpx.RequestError("Network error", request=MagicMock())
    with pytest.raises(httpx.RequestError):
        await http_executor.fetch(sample_request_spec)


@pytest.mark.asyncio
async def test_fetch_timeout_and_empty_response(
    http_executor: HttpExecutor,
    success_response: Response,
    sample_request_spec: RequestSpec,
) -> None:
    mock_async_client = http_executor._client
    mock_async_client._http_limits = HTTPConfig(timeout_s=10.0)
    mock_async_client.run_async.return_value = success_response
    await http_executor.fetch(RequestSpec(method=HttpMethod.GET, url="https://api.example.com/data"))
    _, kwargs = mock_async_client.run_async.call_args
    assert kwargs.get("timeout") == 10.0
    empty = MagicMock(spec=Response)
    empty.status_code = 200
    empty.content = b""
    empty.headers = {}
    mock_async_client.run_async.return_value = empty
    result = await http_executor.fetch(sample_request_spec)
    assert result == b""


@pytest.mark.asyncio
@pytest.mark.skipif(pd is None, reason="requires optional dependency: vertex-forager[yfinance]")
async def test_yfinance_dispatch_ticker_attr_and_download(
    http_executor: HttpExecutor,
    mock_async_client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyTicker:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

        def info(self) -> dict[str, str]:
            return {"ok": "yes"}

    class DummyYF:
        def Ticker(self, sym: str) -> DummyTicker:
            return DummyTicker(sym)

        def download(self, **kwargs: object) -> str:
            return "downloaded"

    monkeypatch.setattr("vertex_forager.core.http.yf", DummyYF())
    mock_async_client.run_sync = AsyncMock(side_effect=lambda func: func())
    spec1 = RequestSpec(
        method=HttpMethod.GET,
        url="yfinance://AAPL",
        params={"dataset": "info", "lib": {"type": "ticker_attr", "attr": "info", "kwargs": {}}},
    )
    out1 = await http_executor.fetch(spec1)
    assert out1.startswith(b"JSON:")
    assert json.loads(out1[5:].decode("utf-8")) == {"ok": "yes"}
    spec2 = RequestSpec(
        method=HttpMethod.GET,
        url="yfinance://AAPL",
        params={"dataset": "price", "lib": {"type": "download", "kwargs": {"period": "1d"}}},
    )
    out2 = await http_executor.fetch(spec2)
    assert json.loads(out2[5:].decode("utf-8")) == "downloaded"


@pytest.mark.asyncio
@pytest.mark.skipif(pd is None, reason="requires optional dependency: vertex-forager[yfinance]")
async def test_yfinance_unknown_or_unsupported_scheme_raises(
    http_executor: HttpExecutor,
    mock_async_client: AsyncMock,
) -> None:
    mock_async_client.run_sync = AsyncMock(side_effect=lambda func: func())
    with pytest.raises(ValueError, match="Unsupported library call type: unknown"):
        await http_executor.fetch(
            RequestSpec(
                method=HttpMethod.GET,
                url="yfinance://AAPL",
                params={"dataset": "price", "lib": {"type": "unknown", "kwargs": {}}},
            )
        )
    with pytest.raises(ValueError, match="Unsupported library scheme: ftp"):
        await http_executor.fetch(
            RequestSpec(
                method=HttpMethod.GET,
                url="ftp://AAPL",
                params={"dataset": "price", "lib": {"type": "ticker_attr", "attr": "info", "kwargs": {}}},
            )
        )


class FakeClient:
    _http_limits: HTTPConfig

    def __init__(self) -> None:
        self._http_limits = HTTPConfig()

    async def run_async(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        req = httpx.Request(method, url)
        return httpx.Response(200, request=req, content=b"ok")

    async def run_sync(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)


@pytest.mark.asyncio
async def test_http_executor_uses_client_protocol() -> None:
    client = FakeClient()
    execu = HttpExecutor(client=client)
    spec = RequestSpec(
        method=HttpMethod.GET,
        url="https://example.com",
        headers={},
        params={},
        auth=RequestAuth(kind="none"),
    )
    payload = await execu.fetch(spec)
    assert payload == b"ok"


@pytest.mark.asyncio
async def test_executor_handles_concurrent_requests(mock_async_client: AsyncMock) -> None:
    mock_async_client._http_limits = HTTPConfig()
    executor = HttpExecutor(client=mock_async_client)
    success_response = MagicMock(spec=Response)
    success_response.status_code = 200
    success_response.content = b'{"data": "success"}'
    mock_async_client.run_async.return_value = success_response
    spec1 = RequestSpec(url="https://api.example.com/data1", method=HttpMethod.GET)
    spec2 = RequestSpec(url="https://api.example.com/data2", method=HttpMethod.GET)
    results = await asyncio.gather(executor.fetch(spec1), executor.fetch(spec2))
    assert len(results) == 2
    assert results[0] == b'{"data": "success"}'
    assert results[1] == b'{"data": "success"}'
    assert mock_async_client.run_async.call_count == 2
