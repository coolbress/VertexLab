from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from vertex_forager.core.config import (
    EngineConfig,
    FetchJob,
    FramePacket,
    ParseResult,
    RequestSpec,
)
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import WriteResult


class StubRouter:
    @property
    def provider(self) -> str:
        return "stub"
    async def generate_jobs(
        self,
        *,
        dataset: str,
        symbols: Sequence[str] | None,
        **kwargs: object,
    ) -> AsyncIterator[FetchJob]:
        yield FetchJob(
            provider="stub",
            dataset=dataset,
            symbol="AAPL",
            spec=RequestSpec(url="https://example.com"),
        )
    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        return ParseResult(packets=[], next_jobs=[])


class StubWriter:
    async def write(self, packet: FramePacket) -> WriteResult:
        return WriteResult(
            table="t",
            rows=int(packet.frame.height),
            partitions={},
        )
    async def flush(self) -> None:
        return None
    async def close(self) -> None:
        return None


class StubMapper:
    def normalize(self, *, packet: FramePacket) -> FramePacket:
        return packet


class _Resp:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code
    def raise_for_status(self) -> None:
        if not (200 <= self.status_code < 300):
            req = httpx.Request("GET", "https://example.com")
            resp = httpx.Response(self.status_code, request=req)
            raise httpx.HTTPStatusError("status", request=req, response=resp)
        return None


class StubClient:
    def __init__(self) -> None:
        self._config = EngineConfig(requests_per_minute=60, structured_logs=True)
        self.controller = FlowController(requests_per_minute=60, concurrency_limit=1)
        self._calls = 0
    async def run_async(self, method: str, url: str, **kwargs: Any) -> _Resp:
        self._calls += 1
        if self._calls == 1:
            return _Resp(b"first", status_code=429)
        return _Resp(b"second", status_code=200)
    @asynccontextmanager
    async def _http_client(self):
        yield None


@pytest.mark.asyncio
async def test_fetch_with_retry_logs_and_succeeds_on_429_then_200(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")
    stub_client = StubClient()
    http = HttpExecutor(client=stub_client)
    pipeline = VertexForager(
        router=StubRouter(),
        http=http,
        writer=StubWriter(),
        mapper=StubMapper(),
        config=stub_client._config,
        controller=stub_client.controller,
    )
    job = FetchJob(
        provider="stub",
        dataset="price",
        symbol="AAPL",
        spec=RequestSpec(url="https://example.com"),
    )
    payload = await pipeline._fetch_with_retry(job)
    assert payload == b"second"
    messages = [
        rec.message for rec in caplog.records if rec.name == "vertex_forager.debug"
    ]
    assert any("stage=http_start" in m for m in messages)
    assert any(
        "stage=http_retry" in m or "stage=http_retry_reason" in m for m in messages
    )
    assert any("stage=http_end" in m for m in messages)
