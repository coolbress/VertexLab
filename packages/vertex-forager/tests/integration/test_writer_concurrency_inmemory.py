from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timezone

import polars as pl

from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, ParseResult, RequestSpec
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.writers.memory import InMemoryBufferWriter


class _Router:
    @property
    def provider(self) -> str:
        return "inmem"

    async def generate_jobs(
        self,
        *,
        dataset: str,
        symbols: Sequence[str] | None,
        **_kwargs: object,
    ) -> AsyncIterator[FetchJob]:
        for symbol in list(symbols or []):
            yield FetchJob(
                provider="inmem",
                dataset=dataset,
                symbol=symbol,
                spec=RequestSpec(url=f"http://example.local/{symbol}"),
            )

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        symbol = str(job.symbol) if job.symbol is not None else "0-0"
        row_text = symbol.split("-", 1)[1] if "-" in symbol else "0"
        try:
            row = int(row_text)
        except ValueError:
            row = 0
        frame = pl.DataFrame({"ticker": [symbol], "value": [row]})
        packet = FramePacket(
            provider="inmem",
            table="price",
            frame=frame,
            observed_at=datetime.now(timezone.utc),
        )
        return ParseResult(packets=[packet], next_jobs=[])


class _FakeHttpClient:
    class _Resp:
        def __init__(self, content: bytes) -> None:
            self.content = content

        def raise_for_status(self) -> None:
            return None

    async def run_async(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        json: dict | None = None,
        content: bytes | None = None,
        timeout: float | None = None,
    ) -> _Resp:
        return _FakeHttpClient._Resp(b"{}")


def test_writer_concurrency_with_inmemory_writer() -> None:
    rpm = 100_000
    writer = InMemoryBufferWriter()
    symbols = [f"T-{i}" for i in range(120)]
    engine = VertexForager(
        router=_Router(),
        http=HttpExecutor(client=_FakeHttpClient()),
        writer=writer,
        mapper=SchemaMapper(),
        config=EngineConfig(
            requests_per_minute=rpm,
            concurrency=6,
            writer_concurrency=2,
        ),
        controller=FlowController(requests_per_minute=rpm, concurrency_limit=6),
    )
    result = asyncio.run(engine.run(dataset="price", symbols=symbols))
    assert len(getattr(engine, "_writer_tasks", [])) == 2
    assert result.tables.get("price", 0) == len(symbols)
    df = writer.collect_table("price", sort_cols=["ticker"])
    assert df.height == len(symbols)
    assert df.get_column("ticker").n_unique() == len(symbols)
