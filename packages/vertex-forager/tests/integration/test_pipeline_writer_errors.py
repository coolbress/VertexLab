from __future__ import annotations

import pytest
import polars as pl
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from collections.abc import Sequence, AsyncIterator
from typing import Any, cast
from pathlib import Path

from vertex_forager.core.config import EngineConfig, RunResult, FetchJob, RequestSpec, ParseResult, FramePacket
from vertex_forager.core.contracts import IRouter, IMapper
from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.core.pipeline import VertexForager
import os
from vertex_forager.core.http import HttpExecutor


class _Resp:
    def __init__(self, content: bytes) -> None:
        self.content = content
    def raise_for_status(self) -> None:
        return None


class StubClient:
    def __init__(self) -> None:
        self._config = EngineConfig(requests_per_minute=60)
        class C:
            def __init__(self) -> None:
                self.concurrency_limit = 1
            @asynccontextmanager
            async def throttle(self) -> AsyncIterator[None]:
                yield None
        self.controller = cast(Any, C())
        self.last_run: RunResult | None = None
    async def run_async(self, method: str, url: str, **kwargs: Any) -> _Resp:
        return _Resp(b"ok")
    @asynccontextmanager
    async def _http_client(self) -> AsyncIterator[None]:
        yield None


class StubRouter(IRouter[str]):
    @property
    def provider(self) -> str:
        return "yfinance"
    async def generate_jobs(self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object) -> AsyncIterator[FetchJob]:
        job = FetchJob(provider="yfinance", dataset=dataset, symbol="AAPL", spec=RequestSpec(url="http://example.com"))
        yield job
    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "close": [1.0], "fetched_at": [datetime.now(timezone.utc)]})
        pkt = FramePacket(provider="yfinance", table="yfinance_price", frame=df, observed_at=datetime.now(timezone.utc))
        return ParseResult(packets=[pkt], next_jobs=[])

class StubMapper(IMapper):
    def normalize(self, *, packet: FramePacket) -> FramePacket:
        return packet

@pytest.mark.asyncio
@pytest.mark.integration
async def test_pipeline_records_writer_validation_errors(tmp_path: Path) -> None:
    os.environ["VERTEXFORAGER_ROOT"] = str(tmp_path / "app")
    client = StubClient()
    router = StubRouter()
    writer = DuckDBWriter(str(tmp_path / "err.duckdb"))
    http = HttpExecutor(client=client)
    pipeline = VertexForager(router=router, http=http, writer=writer, mapper=StubMapper(), config=client._config, controller=client.controller)
    try:
        res = await pipeline.run(dataset="price", symbols=None)
        assert isinstance(res, RunResult)
        assert any(err.startswith("WriterError:yfinance_price:") for err in res.errors)
        assert any("DLQ" in err for err in res.errors)
        # Verify DLQ IPC file exists and is readable
        dlq_dir = tmp_path / "app" / "cache" / "dlq" / "yfinance_price"
        files = list(dlq_dir.glob("batch_*.ipc"))
        assert len(files) >= 1
        df = pl.read_ipc(files[0])
        assert df.shape[0] >= 1
    finally:
        await writer.close()
