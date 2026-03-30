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
from vertex_forager.core.scheduler import FairnessState, SchedulerResult

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
    """Sentinel consumed by _pop_next_job_respecting_fairness sets already_done=True."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(last_symbol=None, burst_count=0)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    await req_q.put((VertexForager.PRIORITY_SENTINEL, 0, None))

    selected = await engine._dequeue_worker_job(
        req_q=req_q, burst_cap=2
    )
    assert selected.job is None
    assert selected.already_done is True
    assert selected.priority == VertexForager.PRIORITY_SENTINEL
    assert selected.demoted == []


@pytest.mark.asyncio
async def test_fairness_burst_cap_demotes_and_finds_different_candidate() -> None:
    """When burst_cap is exceeded, same-symbol jobs are demoted and a different candidate is returned."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(last_symbol="AAPL", burst_count=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    msft_job = FetchJob(provider="stub", dataset="d", symbol="MSFT", spec=RequestSpec(url="https://y"))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 0, aapl_job))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 1, aapl_job))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 2, msft_job))

    selected = await engine._dequeue_worker_job(
        req_q=req_q, burst_cap=2
    )
    assert selected.job is not None
    assert selected.job.symbol == "MSFT"
    assert len(selected.demoted) == 2
    assert all(dj.symbol == "AAPL" for dj in selected.demoted)
    assert not selected.already_done


@pytest.mark.asyncio
async def test_fairness_burst_cap_queue_empty_after_demotes() -> None:
    """When burst_cap exceeded and queue empties during demote drain, return with no candidate."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(last_symbol="AAPL", burst_count=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 0, aapl_job))

    selected = await engine._dequeue_worker_job(
        req_q=req_q, burst_cap=2
    )
    assert selected.job is None
    assert len(selected.demoted) == 1
    assert selected.demoted[0].symbol == "AAPL"
    assert not selected.already_done
    assert selected.priority == VertexForager.PRIORITY_NEW_JOB


@pytest.mark.asyncio
async def test_fairness_sentinel_found_during_demote_drain() -> None:
    """Sentinel is deferred when demoted jobs exist so requeue can happen first."""
    router = _PaginatingRouter(pages=0)
    engine, _ = _make_engine(router)
    engine._fair_lock = asyncio.Lock()
    engine._fair_state = FairnessState(last_symbol="AAPL", burst_count=2)

    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    aapl_job = FetchJob(provider="stub", dataset="d", symbol="AAPL", spec=RequestSpec(url="https://x"))
    # AAPL pagination followed by sentinel
    await req_q.put((VertexForager.PRIORITY_PAGINATION, 0, aapl_job))
    await req_q.put((VertexForager.PRIORITY_SENTINEL, 0, None))

    selected = await engine._dequeue_worker_job(
        req_q=req_q, burst_cap=2
    )
    assert selected.job is None
    assert selected.already_done is False
    assert len(selected.demoted) == 1
    assert selected.demoted[0].symbol == "AAPL"
    p2, _ord, sentinel = req_q.get_nowait()
    assert p2 == VertexForager.PRIORITY_SENTINEL
    assert sentinel is None


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
