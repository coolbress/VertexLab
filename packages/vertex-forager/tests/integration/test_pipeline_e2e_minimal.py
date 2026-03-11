from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator
from collections.abc import Sequence

import polars as pl

from vertex_forager.core.pipeline import VertexForager
from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, RequestSpec, ParseResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.writers.duckdb import DuckDBWriter


class _Router:
    @property
    def provider(self) -> str:
        return "e2e"

    async def generate_jobs(self, *, dataset: str, symbols: Sequence[str] | None, **_kwargs: object) -> AsyncIterator[FetchJob]:
        yield FetchJob(provider="e2e", dataset=dataset, symbol=None, spec=RequestSpec(url="http://example.local/test"))

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        df = pl.DataFrame({"provider": ["e2e"], "value": [1]})
        pkt = FramePacket(provider="e2e", table="e2e_table", frame=df, observed_at=datetime.now(timezone.utc))
        return ParseResult(packets=[pkt], next_jobs=[])


async def _fake_fetch(_self, _spec: RequestSpec) -> bytes:  # pragma: no cover - trivial stub
    return b"{}"


def test_e2e_pipeline_with_duckdb(tmp_path, monkeypatch) -> None:
    # Patch HttpExecutor.fetch to avoid network
    monkeypatch.setattr(HttpExecutor, "fetch", _fake_fetch)

    writer = DuckDBWriter(tmp_path / "e2e.duckdb")
    engine = VertexForager(
        router=_Router(),
        http=HttpExecutor(client=object()),
        writer=writer,
        mapper=SchemaMapper(),
        config=EngineConfig(requests_per_minute=60),
        controller=FlowController(requests_per_minute=60, concurrency_limit=4),
    )
    res = asyncio.run(engine.run(dataset="any", symbols=None))
    assert res.tables.get("e2e_table", 0) == 1
