"""Targeted tests for uncovered pipeline branches.

Covers:
- pagination_max_burst=None (no burst cap — all pages consecutive)
- _pop_next_job_respecting_fairness direct branch coverage
- _try_flush_once idempotent re-entry and suppress paths
- deferred_demotes sentinel cleanup
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest

from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, ParseResult, RequestSpec, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager

# ─── Stubs ──────────────────────────────────────────────────────────────


class _StubClient:
    async def run_async(self, method: str, url: str, **kwargs: Any) -> Any:
        class R:
            content = b"ok"

            def raise_for_status(self) -> None:
                return None

        await asyncio.sleep(0)
        return R()

    @asynccontextmanager
    async def _http_client(self):
        yield None


class _RecordingWriter:
    def __init__(self) -> None:
        self.flush_count = 0

    async def write(self, packet: FramePacket) -> dict[str, Any]:
        return {"table": packet.table, "rows": 0}

    async def write_bulk(self, packets: list[FramePacket]) -> list[Any]:
        return []

    async def flush(self) -> None:
        self.flush_count += 1

    async def close(self) -> None:
        return None


def _make_engine(
    router: Any,
    *,
    concurrency: int = 1,
    pagination_max_burst: int | None = None,
    writer: Any | None = None,
) -> tuple[VertexForager, Any]:
    w = writer or _RecordingWriter()
    config = EngineConfig(
        requests_per_minute=60,
        concurrency=concurrency,
        metrics_enabled=False,
        structured_logs=False,
        pagination_max_burst=pagination_max_burst,
    )
    controller = FlowController(requests_per_minute=60, concurrency_limit=concurrency)
    engine = VertexForager(
        router=router,
        http=HttpExecutor(client=_StubClient()),
        writer=w,
        mapper=None,
        config=config,
        controller=controller,
    )
    return engine, w


# ─── Test 1: pagination_max_burst=None → unlimited consecutive pages ────


class _PaginatingRouter:
    """Router that produces N pagination pages for AAPL."""

    def __init__(self, pages: int = 4) -> None:
        self._provider = "stub"
        self._remaining: dict[str, int] = {"AAPL": pages}

    @property
    def provider(self) -> str:
        return self._provider

    async def generate_jobs(self, *, dataset: str, symbols: list[str] | None, **_: object):
        yield FetchJob(provider=self._provider, dataset=dataset, symbol="AAPL", spec=RequestSpec(url="https://x"))
        yield FetchJob(provider=self._provider, dataset=dataset, symbol="MSFT", spec=RequestSpec(url="https://y"))

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        next_jobs: list[FetchJob] = []
        sym = job.symbol
        if sym is not None and self._remaining.get(sym, 0) > 0:
            self._remaining[sym] -= 1
            next_jobs.append(FetchJob(provider=self._provider, dataset=job.dataset, symbol=sym, spec=job.spec))
        return ParseResult(packets=[], next_jobs=next_jobs)


@pytest.mark.asyncio
async def test_no_burst_cap_allows_all_consecutive_pages() -> None:
    """Without pagination_max_burst, all same-symbol pages run consecutively."""
    router = _PaginatingRouter(pages=4)
    engine, _ = _make_engine(router, pagination_max_burst=None)

    order: list[str] = []

    async def on_progress(
        *, job: FetchJob, payload: bytes | None, exc: Exception | None, parse_result: ParseResult | None
    ) -> None:
        if job.symbol:
            order.append(job.symbol)

    result: RunResult = await engine.run(dataset="d", symbols=["AAPL", "MSFT"], on_progress=on_progress)
    assert isinstance(result, RunResult)

    aapl_indices = [i for i, s in enumerate(order) if s == "AAPL"]
    assert len(aapl_indices) >= 4, f"Expected ≥4 AAPL events, got {len(aapl_indices)}"
    # With burst_cap=None, the engine should process pages without fairness interruption.
    # AAPL should appear in a long consecutive run (initial + 4 pages = 5).
    max_consec = 0
    streak = 0
    current = ""
    for s in order:
        if s == current:
            streak += 1
        else:
            current = s
            streak = 1
        max_consec = max(max_consec, streak)
    assert max_consec >= 4, f"Expected ≥4 consecutive same-symbol events without cap, got {max_consec}"


# ─── Test 2: _pop_next_job_respecting_fairness direct tests ────────────


@pytest.mark.asyncio
async def test_fairness_sentinel_returns_already_done() -> None:
    """Sentinel consumed by _pop_next_job_respecting_fairness sets already_done=True."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_last_symbol = None
    engine._fair_burst_count = 0

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    await req_q.put((VertexForager.PRIORITY_SENTINEL, 0, None))

    priority, job, demote_jobs, already_done = await engine._pop_next_job_respecting_fairness(
        req_q=req_q, burst_cap=2
    )
    assert job is None
    assert already_done is True
    assert priority == VertexForager.PRIORITY_SENTINEL
    assert demote_jobs == []


@pytest.mark.asyncio
async def test_fairness_burst_cap_demotes_and_finds_different_candidate() -> None:
    """When burst_cap is exceeded, same-symbol jobs are demoted and a different candidate is returned."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_last_symbol = "AAPL"
    engine._fair_burst_count = 2

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    msft_job = FetchJob(provider="stub", dataset="d", symbol="MSFT", spec=RequestSpec(url="https://y"))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 0, aapl_job))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 1, aapl_job))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 2, msft_job))

    _priority, job, demote_jobs, already_done = await engine._pop_next_job_respecting_fairness(
        req_q=req_q, burst_cap=2
    )
    assert job is not None
    assert job.symbol == "MSFT"
    assert len(demote_jobs) == 2
    assert all(dj.symbol == "AAPL" for dj in demote_jobs)
    assert not already_done


@pytest.mark.asyncio
async def test_fairness_burst_cap_queue_empty_after_demotes() -> None:
    """When burst_cap exceeded and queue empties during demote drain, return with no candidate."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_last_symbol = "AAPL"
    engine._fair_burst_count = 2

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 0, aapl_job))

    priority, job, demote_jobs, already_done = await engine._pop_next_job_respecting_fairness(
        req_q=req_q, burst_cap=2
    )
    assert job is None
    assert len(demote_jobs) == 1
    assert demote_jobs[0].symbol == "AAPL"
    assert not already_done
    assert priority == VertexForager.PRIORITY_NEW_JOB


@pytest.mark.asyncio
async def test_fairness_sentinel_found_during_demote_drain() -> None:
    """Sentinel encountered during consecutive-symbol drain returns immediately."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_last_symbol = "AAPL"
    engine._fair_burst_count = 2

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    # AAPL pagination followed by sentinel
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 0, aapl_job))
    await req_q.put((VertexForager.PRIORITY_SENTINEL, 0, None))

    _priority, job, demote_jobs, already_done = await engine._pop_next_job_respecting_fairness(
        req_q=req_q, burst_cap=2
    )
    assert job is None
    assert already_done is True
    assert len(demote_jobs) == 1
    assert demote_jobs[0].symbol == "AAPL"


# ─── Test 3: _try_flush_once idempotency ───────────────────────────────


@pytest.mark.asyncio
async def test_flush_once_idempotent_consume_true() -> None:
    """Second call to _try_flush_once(consume=True) does not re-flush."""
    router = _PaginatingRouter(pages=0)
    writer = _RecordingWriter()
    engine, _ = _make_engine(router, writer=writer)
    engine._flush_lock = asyncio.Lock()
    engine._writer_flush_attempted = False
    engine._writer_flushed = False

    await engine._try_flush_once(suppress=False, consume=True)
    assert writer.flush_count == 1
    assert engine._writer_flushed is True

    await engine._try_flush_once(suppress=False, consume=True)
    assert writer.flush_count == 1, "Second flush should be skipped"


@pytest.mark.asyncio
async def test_flush_once_idempotent_consume_false() -> None:
    """consume=False path: flushes once, then skips on _writer_flushed=True."""
    router = _PaginatingRouter(pages=0)
    writer = _RecordingWriter()
    engine, _ = _make_engine(router, writer=writer)
    engine._flush_lock = asyncio.Lock()
    engine._writer_flushed = False

    await engine._try_flush_once(suppress=True, consume=False)
    assert writer.flush_count == 1
    assert engine._writer_flushed is True

    await engine._try_flush_once(suppress=True, consume=False)
    assert writer.flush_count == 1, "Second flush should be skipped when _writer_flushed=True"


@pytest.mark.asyncio
async def test_flush_once_suppress_true_swallows_error() -> None:
    """suppress=True swallows writer.flush() errors."""
    router = _PaginatingRouter(pages=0)

    class _FailingWriter(_RecordingWriter):
        async def flush(self) -> None:
            raise RuntimeError("flush boom")

    writer = _FailingWriter()
    engine, _ = _make_engine(router, writer=writer)
    engine._flush_lock = asyncio.Lock()
    engine._writer_flushed = False

    await engine._try_flush_once(suppress=True, consume=False)
    assert engine._writer_flushed is False

    await engine._try_flush_once(suppress=True, consume=True)
    assert engine._writer_flushed is False


@pytest.mark.asyncio
async def test_flush_once_suppress_false_raises_error() -> None:
    """suppress=False propagates writer.flush() errors."""
    router = _PaginatingRouter(pages=0)

    class _FailingWriter(_RecordingWriter):
        async def flush(self) -> None:
            raise RuntimeError("flush boom")

    writer = _FailingWriter()
    engine, _ = _make_engine(router, writer=writer)
    engine._flush_lock = asyncio.Lock()
    engine._writer_flushed = False

    with pytest.raises(RuntimeError, match="flush boom"):
        await engine._try_flush_once(suppress=False, consume=False)

    engine._writer_flush_attempted = False
    with pytest.raises(RuntimeError, match="flush boom"):
        await engine._try_flush_once(suppress=False, consume=True)


# ─── Test 4: lifecycle guard prevents concurrent run() ──────────────────


@pytest.mark.asyncio
async def test_concurrent_run_raises() -> None:
    """Second concurrent run() call raises RuntimeError."""

    class _InfiniteRouter:
        @property
        def provider(self) -> str:
            return "stub"

        async def generate_jobs(self, *, dataset: str, symbols: list[str] | None, **_: object):
            while True:
                yield FetchJob(provider="stub", dataset=dataset, symbol="AAPL", spec=RequestSpec(url="https://x"))

        def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
            return ParseResult(packets=[], next_jobs=[])

    engine, _ = _make_engine(_InfiniteRouter())
    task = asyncio.create_task(engine.run(dataset="d", symbols=["AAPL"]))
    await asyncio.sleep(0.05)

    with pytest.raises(RuntimeError, match="already running"):
        await engine.run(dataset="d2", symbols=["MSFT"])

    await engine.stop()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
