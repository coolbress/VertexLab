from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, ParseResult, RequestSpec, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager


class FairnessRouter:
    def __init__(self, pages_for_aapl: int = 5) -> None:
        self._provider = "stub"
        self._remaining = {"AAPL": pages_for_aapl, "MSFT": 0}

    @property
    def provider(self) -> str:
        return self._provider

    async def generate_jobs(self, *, dataset: str, symbols: list[str] | None, **_: object):
        yield FetchJob(provider=self._provider, dataset=dataset, symbol="AAPL", spec=RequestSpec(url="https://x"))
        yield FetchJob(provider=self._provider, dataset=dataset, symbol="MSFT", spec=RequestSpec(url="https://y"))

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        next_jobs: list[FetchJob] = []
        sym = job.symbol
        if self._remaining.get(sym, 0) > 0:
            self._remaining[sym] -= 1
            if self._remaining[sym] > 0:
                next_jobs.append(FetchJob(provider=self._provider, dataset=job.dataset, symbol=sym, spec=job.spec))
        return ParseResult(packets=[], next_jobs=next_jobs)


class StubWriter:
    async def write(self, packet: FramePacket):
        return {"table": packet.table, "rows": 0}

    async def write_bulk(self, packets: list[FramePacket]):
        return []

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class StubClient:
    async def run_async(self, method: str, url: str, **kwargs: Any) -> Any:
        class R:
            content = b"ok"

            def raise_for_status(self) -> None:
                return None

        return R()

    @asynccontextmanager
    async def _http_client(self):
        yield None


@pytest.mark.asyncio
async def test_pagination_fairness_cap_prevents_long_bursts() -> None:
    router = FairnessRouter(pages_for_aapl=5)
    http = HttpExecutor(client=StubClient())
    writer = StubWriter()
    config = EngineConfig(
        requests_per_minute=60, concurrency=1, metrics_enabled=False, structured_logs=False, pagination_max_burst=2
    )
    controller = FlowController(requests_per_minute=60, concurrency_limit=1)
    engine = VertexForager(router=router, http=http, writer=writer, mapper=None, config=config, controller=controller)

    order: list[str] = []

    async def on_progress(
        *,
        job: FetchJob,
        payload: bytes | None,
        exc: Exception | None,
        parse_result: ParseResult | None,
    ) -> None:
        assert job.symbol is not None
        order.append(job.symbol)

    res: RunResult = await engine.run(dataset="d", symbols=["AAPL", "MSFT"], on_progress=on_progress)
    assert isinstance(res, RunResult)
    # Ensure we never see more than 2 consecutive AAPL before MSFT appears at least once
    max_consec = 0
    current = ""
    streak = 0
    for s in order:
        if s == current:
            streak += 1
        else:
            current = s
            streak = 1
        max_consec = max(max_consec, streak)
    # Allow initial new-job + burst of 2 pages => max 3 in a row
    assert max_consec <= 3
