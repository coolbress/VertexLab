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
import itertools
from pathlib import Path
import time
from typing import Any

import pytest

from vertex_forager.core import pipeline as pipeline_module
from vertex_forager.core.checkpoint import Checkpoint, save_checkpoint
from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    ParseResult,
    RequestSpec,
    ResolvedClientConfig,
    RunResult,
)
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.core.scheduler import (
    FairnessState,
    SchedulerResult,
    enqueue_pagination_job,
    mark_pagination_job_done,
)

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
    config = ResolvedClientConfig(
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


# ─── Test 2: fairness dequeue tests ─────────────────────────────────────


@pytest.mark.asyncio
async def test_fairness_sentinel_returns_already_done() -> None:
    """When DRR is idle, sentinel still comes from the main request queue."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(quantum=3)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    await req_q.put((VertexForager.PRIORITY_SENTINEL, 0, None))

    selected = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)
    assert selected.job is None
    assert selected.already_done is True
    assert selected.priority == VertexForager.PRIORITY_SENTINEL
    assert selected.demoted == []


@pytest.mark.asyncio
async def test_fairness_burst_cap_demotes_and_finds_different_candidate() -> None:
    """DRR rotates to a different symbol after the configured quantum."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(quantum=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    msft_job = FetchJob(provider="stub", dataset="d", symbol="MSFT", spec=RequestSpec(url="https://y"))
    await enqueue_pagination_job(fair_lock=engine._fair_lock, fairness_state=engine._fair_state, job=aapl_job)
    await enqueue_pagination_job(fair_lock=engine._fair_lock, fairness_state=engine._fair_state, job=aapl_job)
    await enqueue_pagination_job(fair_lock=engine._fair_lock, fairness_state=engine._fair_state, job=aapl_job)
    await enqueue_pagination_job(fair_lock=engine._fair_lock, fairness_state=engine._fair_state, job=msft_job)

    first = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)
    second = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)
    await mark_pagination_job_done(fair_lock=engine._fair_lock, fairness_state=engine._fair_state)
    await mark_pagination_job_done(fair_lock=engine._fair_lock, fairness_state=engine._fair_state)

    selected = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)
    assert first.job is not None
    assert first.job.symbol == "AAPL"
    assert second.job is not None
    assert second.job.symbol == "AAPL"
    assert selected.job is not None
    assert selected.job.symbol == "MSFT"
    assert selected.demoted == []
    assert not selected.already_done
    await mark_pagination_job_done(fair_lock=engine._fair_lock, fairness_state=engine._fair_state)


@pytest.mark.asyncio
async def test_fairness_burst_cap_queue_empty_after_demotes() -> None:
    """When DRR is enabled and idle, dequeue falls back to the main queue."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(quantum=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    await req_q.put(
        (
            VertexForager.PRIORITY_NEW_JOB,
            0,
            FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x")),
        )
    )

    selected = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)
    assert selected.job is not None
    assert selected.job.symbol == "AAPL"
    assert selected.demoted == []
    assert not selected.already_done
    assert selected.priority == VertexForager.PRIORITY_NEW_JOB


@pytest.mark.asyncio
async def test_fairness_sentinel_found_during_demote_drain() -> None:
    """Main-queue sentinels take priority over DRR work during shutdown-style dequeue."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(quantum=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    await enqueue_pagination_job(fair_lock=engine._fair_lock, fairness_state=engine._fair_state, job=aapl_job)
    await req_q.put((VertexForager.PRIORITY_SENTINEL, 0, None))

    first = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)
    assert first.job is None
    assert first.already_done is True
    assert first.priority == VertexForager.PRIORITY_SENTINEL


@pytest.mark.asyncio
async def test_fairness_prefers_req_get_when_both_waits_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(quantum=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()

    async def _immediate_pagination_wait(*, fairness_state: FairnessState) -> None:
        return None

    async def _immediate_req_get(
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    ) -> tuple[int, int, FetchJob | None]:
        return (VertexForager.PRIORITY_SENTINEL, 0, None)

    monkeypatch.setattr(
        pipeline_module,
        "wait_for_pagination_availability_impl",
        _immediate_pagination_wait,
        raising=True,
    )
    monkeypatch.setattr(engine, "_get_request_queue", _immediate_req_get, raising=True)
    monkeypatch.setattr(req_q, "task_done", lambda: None, raising=True)

    selected = await engine._dequeue_worker_job(req_q=req_q, burst_cap=2)

    assert selected.job is None
    assert selected.already_done is True
    assert selected.priority == VertexForager.PRIORITY_SENTINEL


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


def test_find_latest_checkpoint_returns_most_recent_by_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    root = tmp_path / "vf-root"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(root))
    with monkeypatch.context() as context:
        context.setattr(
            "vertex_forager.core.checkpoint.time.time",
            lambda: 100.0,
            raising=False,
        )
        save_checkpoint(Checkpoint(run_id="stub_d_100", provider="stub", dataset="d"))
    with monkeypatch.context() as context:
        context.setattr(
            "vertex_forager.core.checkpoint.time.time",
            lambda: 200.0,
            raising=False,
        )
        save_checkpoint(Checkpoint(run_id="stub_d_200", provider="stub", dataset="d"))
    cp = engine._find_latest_checkpoint("stub", "d")
    assert cp is not None
    assert cp.run_id == "stub_d_200"


@pytest.mark.asyncio
async def test_producer_resumes_pending_pagination_jobs_without_restarting_symbol() -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    pending_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )
    order_counter = itertools.count()

    await engine._producer(
        req_q=req_q,
        dataset="d",
        symbols=["AAPL", "MSFT"],
        order_counter=order_counter,
        completed_symbols=set(),
        pending_jobs=[pending_job],
    )

    queued_jobs: list[FetchJob] = []
    while not req_q.empty():
        _priority, _order, queued_job = req_q.get_nowait()
        if queued_job is not None:
            queued_jobs.append(queued_job)

    assert [job.symbol for job in queued_jobs] == ["AAPL", "MSFT"]
    assert queued_jobs[0].spec.params["page"] == 2


@pytest.mark.asyncio
async def test_producer_restores_only_requested_pending_jobs() -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    pending_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )

    async def _generate_jobs(**kwargs: Any):
        yield FetchJob(
            provider="stub",
            dataset="d",
            symbol="MSFT",
            spec=RequestSpec(url="https://x"),
        )

    engine._router.generate_jobs = _generate_jobs  # type: ignore[method-assign]

    await engine._producer(
        req_q=req_q,
        dataset="d",
        symbols=["MSFT"],
        order_counter=itertools.count(),
        completed_symbols=set(),
        pending_jobs=[pending_job],
    )

    queued_jobs: list[FetchJob] = []
    while not req_q.empty():
        _priority, _order, queued_job = req_q.get_nowait()
        if queued_job is not None:
            queued_jobs.append(queued_job)

    assert [job.symbol for job in queued_jobs] == ["MSFT"]


@pytest.mark.asyncio
async def test_producer_preserves_symbolless_pending_jobs_on_resume() -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    pending_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol=None,
        spec=RequestSpec(url="https://x", params={"cursor": "abc"}),
    )

    async def _generate_jobs(**kwargs: Any):
        yield FetchJob(
            provider="stub",
            dataset="d",
            symbol="MSFT",
            spec=RequestSpec(url="https://x"),
        )

    engine._router.generate_jobs = _generate_jobs  # type: ignore[method-assign]

    await engine._producer(
        req_q=req_q,
        dataset="d",
        symbols=["MSFT"],
        order_counter=itertools.count(),
        completed_symbols=set(),
        pending_jobs=[pending_job],
    )

    queued_jobs: list[FetchJob] = []
    while not req_q.empty():
        _priority, _order, queued_job = req_q.get_nowait()
        if queued_job is not None:
            queued_jobs.append(queued_job)

    assert [job.symbol for job in queued_jobs] == [None, "MSFT"]


@pytest.mark.asyncio
async def test_initialize_run_state_preserves_flat_pending_jobs_on_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    dataset_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol=None,
        spec=RequestSpec(url="https://x", params={"cursor": "abc"}),
    )
    symbol_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )
    save_checkpoint(
        Checkpoint(
            run_id="rid",
            provider="stub",
            dataset="d",
            pending_jobs=[dataset_job, symbol_job],
            status="in_progress",
        )
    )

    run_id, completed_symbols = engine._initialize_run_state(dataset="d", resume=True)

    assert run_id == "rid"
    assert completed_symbols == set()
    assert engine._pending_jobs == [dataset_job, symbol_job]


@pytest.mark.asyncio
async def test_record_worker_symbol_state_persists_pending_pagination_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._run_id = "rid"
    engine._checkpoint_lock = asyncio.Lock()
    next_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )
    parse_result = ParseResult(packets=[], next_jobs=[next_job])

    await engine._record_worker_symbol_state(
        job=FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x")),
        worker_exc=None,
        parse_result=parse_result,
    )

    assert "AAPL" not in engine._completed_symbols
    assert "AAPL" not in engine._failed_symbols
    assert engine._pending_jobs[0].spec.params["page"] == 2


@pytest.mark.asyncio
async def test_record_worker_symbol_state_merges_next_jobs_with_existing_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._run_id = "rid"
    engine._checkpoint_lock = asyncio.Lock()
    current_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    existing_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol=None,
        spec=RequestSpec(url="https://x", params={"cursor": "prev"}),
    )
    next_job_symbol = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )
    next_job_dataset = FetchJob(
        provider="stub",
        dataset="d",
        symbol=None,
        spec=RequestSpec(url="https://x", params={"cursor": "next"}),
    )
    engine._pending_jobs = [current_job, existing_job]
    parse_result = ParseResult(packets=[], next_jobs=[next_job_symbol, next_job_dataset])

    await engine._record_worker_symbol_state(
        job=current_job,
        worker_exc=None,
        parse_result=parse_result,
    )

    assert engine._pending_jobs == [existing_job, next_job_symbol, next_job_dataset]


@pytest.mark.asyncio
async def test_record_worker_symbol_state_keeps_pending_jobs_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._run_id = "rid"
    engine._checkpoint_lock = asyncio.Lock()
    pending_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )
    engine._pending_jobs = [pending_job]
    engine._completed_symbols = {"AAPL"}

    await engine._record_worker_symbol_state(
        job=FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x")),
        worker_exc=RuntimeError("boom"),
        parse_result=None,
    )

    assert "AAPL" not in engine._completed_symbols
    assert "AAPL" in engine._failed_symbols
    assert engine._pending_jobs[0].spec.params["page"] == 2


@pytest.mark.asyncio
async def test_record_worker_symbol_state_retries_current_job_after_emit_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._run_id = "rid"
    engine._checkpoint_lock = asyncio.Lock()
    next_job = FetchJob(
        provider="stub",
        dataset="d",
        symbol="AAPL",
        spec=RequestSpec(url="https://x", params={"page": 2}),
    )
    parse_result = ParseResult(packets=[], next_jobs=[next_job])
    current_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))

    await engine._record_worker_symbol_state(
        job=current_job,
        worker_exc=RuntimeError("emit failed"),
        parse_result=parse_result,
    )

    assert "AAPL" not in engine._completed_symbols
    assert "AAPL" in engine._failed_symbols
    assert engine._pending_jobs == [current_job]


@pytest.mark.asyncio
async def test_finalize_run_metrics_sink_and_history_error_suppressed(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._metrics_enabled = True
    engine._counters = {"rows_written_total": 0}
    engine._hists = {}
    engine._summary = {}
    engine._completed_symbols = {"AAPL"}
    engine._failed_symbols = {"MSFT"}
    engine._checkpoint_lock = asyncio.Lock()

    class _Sink:
        @staticmethod
        def summary(_payload: dict[str, float]) -> None:
            raise RuntimeError("sink boom")

    engine._metrics_sink = _Sink()

    async def _noop_flush(*, suppress: bool, consume: bool = True) -> None:
        return None

    monkeypatch.setattr(engine, "_try_flush_once", _noop_flush, raising=True)
    monkeypatch.setattr(engine, "_merge_component_counters", lambda: None, raising=True)
    monkeypatch.setattr(engine, "_compute_summary", lambda: {"http_duration_s_p95": 1.0}, raising=True)
    monkeypatch.setattr(engine, "_emit_pipeline_summary_log", lambda **kwargs: None, raising=True)
    monkeypatch.setattr(engine, "_update_checkpoint", lambda *args, **kwargs: None, raising=True)
    import vertex_forager.core.pipeline as pipeline_mod

    def _save_run_history_fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("history boom")

    monkeypatch.setattr(pipeline_mod, "save_run_history", _save_run_history_fail)
    result = RunResult(provider="stub")
    await engine._finalize_run(result=result, dataset="d", run_id="rid", started_monotonic=time.monotonic() - 1.0)
    assert result.metrics_summary.get("http_duration_s_p95") == 1.0
    assert result.finished_at is not None


@pytest.mark.asyncio
async def test_finalize_run_keeps_checkpoint_resumable_when_failures_remain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._metrics_enabled = False
    engine._completed_symbols = {"AAPL"}
    engine._failed_symbols = {"MSFT"}
    engine._pending_jobs = []
    engine._checkpoint_lock = asyncio.Lock()
    root = tmp_path / "vf-root"

    async def _noop_flush(*, suppress: bool, consume: bool = True) -> None:
        return None

    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(root))
    engine._run_id = "rid"
    result = RunResult(provider="stub")
    monkeypatch.setattr(engine, "_try_flush_once", _noop_flush, raising=True)
    await engine._finalize_run(result=result, dataset="d", run_id="rid", started_monotonic=time.monotonic() - 1.0)
    checkpoint = engine._find_latest_checkpoint("stub", "d")
    assert checkpoint is not None
    assert checkpoint.status == "in_progress"
    assert checkpoint.failed == ["MSFT"]


@pytest.mark.asyncio
async def test_fetch_worker_handles_none_job_then_sentinel(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)

    class _Req:
        @staticmethod
        def task_done() -> None:
            return None

    req_q = _Req()
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="stub")
    lock = asyncio.Lock()
    order_counter = itertools.count()
    steps = [
        SchedulerResult(priority=0, job=None, demoted=[], already_done=False),
        SchedulerResult(priority=engine.PRIORITY_SENTINEL, job=None, demoted=[], already_done=True),
    ]

    async def _dequeue_worker_job(**kwargs: object):
        return steps.pop(0)

    monkeypatch.setattr(engine, "_dequeue_worker_job", _dequeue_worker_job, raising=True)
    monkeypatch.setattr(engine, "_drain_deferred_demotes", lambda **kwargs: None, raising=True)
    monkeypatch.setattr(engine, "_requeue_demoted_jobs", lambda **kwargs: None, raising=True)

    async def _process_worker_job(**kwargs: object):
        return b"x", None, ParseResult(packets=[], next_jobs=[])

    monkeypatch.setattr(engine, "_process_worker_job", _process_worker_job, raising=True)

    async def _record_worker_symbol_state(**kwargs: object) -> None:
        return None

    monkeypatch.setattr(engine, "_record_worker_symbol_state", _record_worker_symbol_state, raising=True)
    sleep_calls = {"n": 0}

    async def _sleep(_delay: float) -> None:
        sleep_calls["n"] += 1

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    await engine._fetch_worker(
        0,
        req_q=req_q,  # type: ignore[arg-type]
        pkt_q=pkt_q,
        result=result,
        result_lock=lock,
        order_counter=order_counter,
        on_progress=None,
    )
    assert sleep_calls["n"] == 1


@pytest.mark.asyncio
async def test_fetch_worker_cancelled_error_preserves_handler_state(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)

    class _Req:
        @staticmethod
        def task_done() -> None:
            return None

    req_q = _Req()
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="stub")
    lock = asyncio.Lock()
    order_counter = itertools.count()
    job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    seen: dict[str, Any] = {}

    async def _dequeue_worker_job(**kwargs: object):
        return SchedulerResult(priority=engine.PRIORITY_NEW_JOB, job=job, demoted=[], already_done=False)

    async def _process_worker_job(**kwargs: object):
        raise asyncio.CancelledError()

    async def on_progress(
        *, job: FetchJob, payload: bytes | None, exc: Exception | None, parse_result: ParseResult | None
    ) -> None:
        seen["payload"] = payload
        seen["exc"] = exc
        seen["parse_result"] = parse_result

    async def _record_worker_symbol_state(**kwargs: object) -> None:
        seen["record_exc"] = kwargs["worker_exc"]
        seen["record_parse_result"] = kwargs["parse_result"]

    monkeypatch.setattr(engine, "_dequeue_worker_job", _dequeue_worker_job, raising=True)
    monkeypatch.setattr(engine, "_drain_deferred_demotes", lambda **kwargs: None, raising=True)
    monkeypatch.setattr(engine, "_requeue_demoted_jobs", lambda **kwargs: None, raising=True)
    monkeypatch.setattr(engine, "_process_worker_job", _process_worker_job, raising=True)
    monkeypatch.setattr(engine, "_record_worker_symbol_state", _record_worker_symbol_state, raising=True)

    with pytest.raises(asyncio.CancelledError):
        await engine._fetch_worker(
            0,
            req_q=req_q,  # type: ignore[arg-type]
            pkt_q=pkt_q,
            result=result,
            result_lock=lock,
            order_counter=order_counter,
            on_progress=on_progress,
        )

    assert seen["payload"] is None
    assert isinstance(seen["exc"], asyncio.CancelledError)
    assert seen["parse_result"] is None
    assert isinstance(seen["record_exc"], asyncio.CancelledError)
    assert seen["record_parse_result"] is None


@pytest.mark.asyncio
async def test_fetch_worker_logs_every_100_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)

    class _Req:
        @staticmethod
        def task_done() -> None:
            return None

    req_q = _Req()
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="stub")
    lock = asyncio.Lock()
    order_counter = itertools.count()
    jobs = [
        SchedulerResult(
            priority=engine.PRIORITY_NEW_JOB,
            job=FetchJob(provider="stub", dataset="d", symbol=f"S{i}", spec=RequestSpec(url="https://x")),
            demoted=[],
            already_done=False,
        )
        for i in range(100)
    ]
    jobs.append(SchedulerResult(priority=engine.PRIORITY_SENTINEL, job=None, demoted=[], already_done=True))

    async def _dequeue_worker_job(**kwargs: object):
        return jobs.pop(0)

    monkeypatch.setattr(engine, "_dequeue_worker_job", _dequeue_worker_job, raising=True)
    monkeypatch.setattr(engine, "_drain_deferred_demotes", lambda **kwargs: None, raising=True)
    monkeypatch.setattr(engine, "_requeue_demoted_jobs", lambda **kwargs: None, raising=True)

    async def _process_worker_job(**kwargs: object):
        return b"x", None, ParseResult(packets=[], next_jobs=[])

    monkeypatch.setattr(engine, "_process_worker_job", _process_worker_job, raising=True)

    async def _record_worker_symbol_state(**kwargs: object) -> None:
        return None

    monkeypatch.setattr(engine, "_record_worker_symbol_state", _record_worker_symbol_state, raising=True)
    debug_msgs: list[str] = []
    import vertex_forager.core.pipeline as pipeline_mod

    monkeypatch.setattr(
        pipeline_mod.logger,
        "debug",
        lambda msg, *args: debug_msgs.append(str(msg)),
        raising=True,
    )
    await engine._fetch_worker(
        0,
        req_q=req_q,  # type: ignore[arg-type]
        pkt_q=pkt_q,
        result=result,
        result_lock=lock,
        order_counter=order_counter,
        on_progress=None,
    )
    assert any("Processed %s jobs so far..." in m for m in debug_msgs)
