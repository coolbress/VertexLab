from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

import pytest

from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    ParseResult,
    RequestSpec,
    ResolvedClientConfig,
)
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import WriteResult


class _StubClient:
    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_stop_no_active_tasks() -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=0)
    eng = VertexForager(  # type: ignore[arg-type]
        router=None, http=HttpExecutor(client=_StubClient()), writer=None, mapper=None, config=cfg, controller=ctrl
    )
    eng._active_tasks = []  # type: ignore[attr-defined]
    await eng.stop()


@pytest.mark.asyncio
async def test_stop_with_completed_writer_tasks() -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(  # type: ignore[arg-type]
        router=None, http=HttpExecutor(client=_StubClient()), writer=None, mapper=None, config=cfg, controller=ctrl
    )

    async def _done():
        return None

    t = asyncio.create_task(_done())
    await t
    # Have a pending active task to avoid early return and exercise cleanup
    pending = asyncio.Event()
    active = asyncio.create_task(pending.wait())
    eng._active_tasks = [active]  # type: ignore[attr-defined]
    eng._writer_tasks = [t]  # type: ignore[attr-defined]
    await eng.stop()
    assert t.done()
    assert not t.cancelled()
    assert active.cancelled()
    # Cleanup pending task
    pending.set()
    with suppress(asyncio.CancelledError):
        await active


@pytest.mark.asyncio
async def test_stop_req_q_put_nowait_allows_no_async_schedule() -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=2)
    eng = VertexForager(  # type: ignore[arg-type]
        router=None, http=HttpExecutor(client=_StubClient()), writer=None, mapper=None, config=cfg, controller=ctrl
    )
    q: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=10)
    eng._req_q = q  # type: ignore[attr-defined]
    active_task = asyncio.create_task(asyncio.sleep(0))
    eng._active_tasks = [active_task]  # type: ignore[attr-defined]
    await eng.stop()
    await asyncio.sleep(0)
    items = []
    try:
        while True:
            items.append(q.get_nowait())
    except asyncio.QueueEmpty:
        pass
    assert all(item[0] == eng.PRIORITY_SENTINEL for item in items)
    assert len(items) <= 2
    # Ensure active task finishes to avoid pending task warnings
    with suppress(asyncio.CancelledError):
        await active_task


@pytest.mark.asyncio
async def test_stop_pkt_q_timeout_cancels_writers(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(  # type: ignore[arg-type]
        router=None, http=HttpExecutor(client=_StubClient()), writer=None, mapper=None, config=cfg, controller=ctrl
    )
    q: asyncio.Queue = asyncio.Queue(maxsize=1)
    q.put_nowait(None)
    eng._pkt_q = q  # type: ignore[attr-defined]

    async def _sleep_forever():
        await asyncio.sleep(3600)

    w = asyncio.create_task(_sleep_forever())
    eng._writer_tasks = [w]  # type: ignore[attr-defined]
    # Have a pending active task to ensure _stop_impl path executes
    pending = asyncio.Event()
    active = asyncio.create_task(pending.wait())
    eng._active_tasks = [active]  # type: ignore[attr-defined]

    async def _raise_timeout(awaitable, timeout):  # type: ignore[no-untyped-def]
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", _raise_timeout, raising=True)
    await eng.stop()
    await asyncio.sleep(0)
    # Writer task should be cancelled by stop()
    assert w.cancelled()
    assert w.done()
    # Cleanup pending active task
    pending.set()
    with suppress(asyncio.CancelledError):
        await active


class SlowRouter:
    @property
    def provider(self) -> str:
        return "stub"

    async def generate_jobs(self, *, dataset: str, symbols: list[str] | None, **_: object) -> AsyncIterator[FetchJob]:
        while True:
            yield FetchJob(provider="stub", dataset=dataset, symbol="AAPL", spec=RequestSpec(url="https://slow"))

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        return ParseResult(packets=[], next_jobs=[])


class FlushCountingWriter:
    def __init__(self) -> None:
        self.flush_called = 0

    async def write(self, packet: FramePacket) -> WriteResult:
        return WriteResult(table=packet.table, rows=0)

    async def write_bulk(self, packets: list[FramePacket]) -> list[WriteResult]:
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
    async def _http_client(self) -> AsyncIterator[None]:
        yield None


@pytest.mark.asyncio
async def test_stop_triggers_writer_flush() -> None:
    router = SlowRouter()
    writer = FlushCountingWriter()
    http = HttpExecutor(client=StubClient())
    config = ResolvedClientConfig(
        requests_per_minute=60,
        concurrency=1,
        metrics_enabled=False,
        structured_logs=False,
    )
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
        else:
            await task

    await run_and_cancel()
    assert writer.flush_called >= 1
