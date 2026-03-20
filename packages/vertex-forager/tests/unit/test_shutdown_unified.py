from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest

from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, ParseResult, RequestSpec
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager


class SlowRouter:
    @property
    def provider(self) -> str:
        return "stub"

    async def generate_jobs(self, *, dataset: str, symbols: list[str] | None, **_: object):
        while True:
            yield FetchJob(provider="stub", dataset=dataset, symbol="AAPL", spec=RequestSpec(url="https://slow"))

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        return ParseResult(packets=[], next_jobs=[])


class FlushCountingWriter:
    def __init__(self) -> None:
        self.flush_called = 0

    async def write(self, packet: FramePacket):
        return {"table": packet.table, "rows": 0}

    async def write_bulk(self, packets: list[FramePacket]):
        return []

    async def flush(self) -> None:
        self.flush_called += 1

    async def close(self) -> None:
        return None


class StubClient:
    async def run_async(self, method: str, url: str, **kwargs: Any) -> Any:
        class R:
            content = b"ok"

            def raise_for_status(self) -> None:
                return None

        await asyncio.sleep(0.01)
        return R()

    @asynccontextmanager
    async def _http_client(self):
        yield None


@pytest.mark.asyncio
async def test_stop_triggers_writer_flush() -> None:
    router = SlowRouter()
    writer = FlushCountingWriter()
    http = HttpExecutor(client=StubClient())
    config = EngineConfig(requests_per_minute=60, concurrency=1, metrics_enabled=False, structured_logs=False)
    controller = FlowController(requests_per_minute=60, concurrency_limit=1)
    engine = VertexForager(router=router, http=http, writer=writer, mapper=None, config=config, controller=controller)

    async def run_and_cancel():
        task = asyncio.create_task(engine.run(dataset="d", symbols=["AAPL"]))
        await asyncio.sleep(0.05)
        await engine.stop()
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    await run_and_cancel()
    assert writer.flush_called >= 1
