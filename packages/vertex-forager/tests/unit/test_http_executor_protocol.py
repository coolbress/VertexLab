from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from vertex_forager.core.config import HttpMethod, RequestAuth, RequestSpec
from vertex_forager.core.http import HttpExecutor


class FakeClient:
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
