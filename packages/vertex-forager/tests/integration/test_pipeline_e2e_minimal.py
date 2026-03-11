from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator
from collections.abc import Sequence

import duckdb
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


class _FakeHttpClient:
    # Hook to exercise before_http_request path: inject test header
    def before_http_request(self, spec: RequestSpec) -> RequestSpec:
        updated_headers = dict(spec.headers)
        updated_headers["X-Test-Hook"] = "1"
        return spec.model_copy(update={"headers": updated_headers})

    class _Resp:
        def __init__(self, content: bytes):
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
    ) -> "_FakeHttpClient._Resp":
        # Return a simple JSON-ish body; router.parse ignores payload content in this test
        # Validate that before_http_request injection propagated into headers
        if headers is None or headers.get("X-Test-Hook") != "1":
            raise AssertionError("before_http_request header not propagated to HttpExecutor._fetch_http")
        return _FakeHttpClient._Resp(b"{}")


def test_e2e_pipeline_with_duckdb(tmp_path) -> None:
    db_path = tmp_path / "e2e.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        engine = VertexForager(
            router=_Router(),
            http=HttpExecutor(client=_FakeHttpClient()),
            writer=writer,
            mapper=SchemaMapper(),
            config=EngineConfig(requests_per_minute=60),
            controller=FlowController(requests_per_minute=60, concurrency_limit=4),
        )
        res = asyncio.run(engine.run(dataset="any", symbols=None))
        assert res.tables.get("e2e_table", 0) == 1
    finally:
        # Ensure DuckDB connection is closed even on failure
        asyncio.run(writer.close())
    with duckdb.connect(str(db_path)) as con:
        row = con.execute("SELECT count(*) FROM e2e_table").fetchone()
        assert row is not None
        cnt = row[0]
        assert cnt == res.tables.get("e2e_table", 0) == 1
