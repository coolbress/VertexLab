from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

import polars as pl
import pytest

from vertex_forager.clients.dispatcher import run_pipeline_for
from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    ParseResult,
    RequestSpec,
    ResolvedClientConfig,
    RunResult,
)
from vertex_forager.core.contracts import BaseMapper, IWriter
from vertex_forager.exceptions import InputError
from vertex_forager.routers.base import BaseRouter
from vertex_forager.writers.base import WriteResult


class StubRouter(BaseRouter[str]):
    @property
    def provider(self) -> str:
        return "stub"

    async def generate_jobs(
        self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        if not symbols:
            return
        job = FetchJob(
            provider="stub",
            dataset=dataset,
            symbol=symbols[0],
            spec=RequestSpec(url="http://example.com"),
        )
        yield job

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        df = pl.DataFrame({"x": [1]})
        pkt = FramePacket(
            provider="stub",
            table="select",
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        return ParseResult(packets=[pkt], next_jobs=[])


class StubWriter(IWriter):
    async def write(self, packet: FramePacket) -> WriteResult:
        return WriteResult(table=packet.table, rows=packet.frame.height)

    async def flush(self) -> None:
        return None


class StubMapper(BaseMapper):
    def normalize(self, *, packet: FramePacket) -> FramePacket:
        return packet


class StubClient:
    def __init__(self) -> None:
        self._config = ResolvedClientConfig(requests_per_minute=60)

        @dataclass
        class StubController:
            concurrency_limit: int

        self.controller = StubController(concurrency_limit=1)
        self.last_run: RunResult | None = None

    @asynccontextmanager
    async def _http_client(self) -> AsyncIterator[None]:
        yield None


@pytest.mark.asyncio
async def test_dispatcher_runs_with_empty_symbols() -> None:
    client = StubClient()
    router = StubRouter()
    writer = StubWriter()
    mapper = StubMapper()
    res = await run_pipeline_for(
        client=client,
        router=router,
        dataset="test",
        symbols=None,
        writer=writer,
        mapper=mapper,
        table_name="stub_table",
    )
    assert isinstance(res, RunResult)
    assert res.provider == "stub"


@pytest.mark.asyncio
async def test_dispatcher_rejects_removed_resume_kwarg() -> None:
    client = StubClient()
    router = StubRouter()
    writer = StubWriter()
    mapper = StubMapper()

    with pytest.raises(InputError, match="StateManager"):
        await run_pipeline_for(
            client=client,
            router=router,
            dataset="test",
            symbols=["AAPL"],
            writer=writer,
            mapper=mapper,
            resume=True,
        )
