from __future__ import annotations

import pytest
from contextlib import asynccontextmanager
import polars as pl

from vertex_forager.core.config import EngineConfig, RunResult, FetchJob, RequestSpec, ParseResult, FramePacket
from vertex_forager.core.contracts import IRouter, IWriter, IMapper
from vertex_forager.clients.dispatcher import run_pipeline_for
from datetime import datetime, timezone
from collections.abc import Sequence


class StubRouter(IRouter[str]):
    @property
    def provider(self) -> str:
        return "stub"

    async def generate_jobs(self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object):
        if not symbols:
            return
        job = FetchJob(provider="stub", dataset=dataset, symbol=symbols[0], spec=RequestSpec(url="http://example.com"))
        yield job

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        df = pl.DataFrame({"x": [1]})
        pkt = FramePacket(provider="stub", table="select", frame=df, observed_at=datetime.now(timezone.utc))
        return ParseResult(packets=[pkt], next_jobs=[])


class StubWriter(IWriter):
    async def write(self, packet: FramePacket):
        return type("WR", (), {"table": packet.table, "rows": packet.frame.height})()
    async def flush(self) -> None:
        return None


class StubMapper(IMapper):
    def normalize(self, *, packet: FramePacket) -> FramePacket:
        return packet


class StubClient:
    def __init__(self) -> None:
        self._config = EngineConfig(requests_per_minute=60)
        self.controller = type("C", (), {"concurrency_limit": 1})()
        self.last_run: RunResult | None = None

    @asynccontextmanager
    async def _http_client(self):
        yield None


@pytest.mark.asyncio
async def test_dispatcher_runs_with_empty_symbols() -> None:
    client = StubClient()
    router = StubRouter()
    writer = StubWriter()
    mapper = StubMapper()
    res = await run_pipeline_for(client=client, router=router, dataset="test", symbols=None, writer=writer, mapper=mapper)
    assert isinstance(res, RunResult)
    assert res.provider == "stub"
