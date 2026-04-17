from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime
import itertools

import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, FramePacket, ParseResult, RequestSpec, RunResult
from vertex_forager.core.workerio import emit_packets_and_next_jobs, parse_payload, record_worker_error


def _job(symbol: str) -> FetchJob:
    return FetchJob(
        provider="sharadar",
        dataset="price",
        symbol=symbol,
        spec=RequestSpec(url=f"https://example.test/{symbol}", params={"ticker": symbol}),
    )


@pytest.mark.asyncio
async def test_emit_packets_and_next_jobs_emits_and_enqueues() -> None:
    pkt = FramePacket(
        provider="sharadar",
        table="sharadar_price",
        frame=pl.DataFrame({"ticker": ["AAPL"]}),
        observed_at=datetime.now(),
    )
    parse_result = ParseResult(packets=[pkt], next_jobs=[_job("MSFT")])
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    counters: dict[str, int] = {}
    enqueued: list[FetchJob] = []

    def _inc(name: str, amount: int) -> None:
        counters[name] = counters.get(name, 0) + amount

    async def _capture_enqueue(job: FetchJob) -> None:
        enqueued.append(job)

    await emit_packets_and_next_jobs(
        parse_result=parse_result,
        req_q=req_q,
        pkt_q=pkt_q,
        order_counter=itertools.count(),
        worker_id=1,
        job=_job("AAPL"),
        parse_executor=None,
        normalize_packet=lambda packet: packet,
        inc=_inc,
        priority_pagination=1,
        enqueue_pagination_job=_capture_enqueue,
        logger=type("L", (), {"debug": lambda *args, **kwargs: None})(),
    )
    assert counters["packets_emitted"] == 1
    assert pkt_q.get_nowait().table == "sharadar_price"
    assert req_q.empty()
    assert len(enqueued) == 1
    assert enqueued[0].symbol == "MSFT"


@pytest.mark.asyncio
async def test_emit_packets_and_next_jobs_batches_normalize_executor_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    packets = [
        FramePacket(
            provider="sharadar",
            table="sharadar_price",
            frame=pl.DataFrame({"ticker": ["AAPL"]}),
            observed_at=datetime.now(),
        ),
        FramePacket(
            provider="sharadar",
            table="sharadar_price",
            frame=pl.DataFrame({"ticker": ["MSFT"]}),
            observed_at=datetime.now(),
        ),
    ]
    parse_result = ParseResult(packets=packets, next_jobs=[])
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    calls = {"executor": 0}

    async def _capture_enqueue(job: FetchJob) -> None:
        raise AssertionError(f"unexpected pagination enqueue: {job}")

    class _Loop:
        async def run_in_executor(self, executor: object, func: object) -> object:
            del executor
            calls["executor"] += 1
            return func()

    monkeypatch.setattr("vertex_forager.core.workerio.asyncio.get_running_loop", lambda: _Loop())

    await emit_packets_and_next_jobs(
        parse_result=parse_result,
        req_q=asyncio.PriorityQueue(),
        pkt_q=pkt_q,
        order_counter=itertools.count(),
        worker_id=1,
        job=_job("AAPL"),
        parse_executor=object(),
        normalize_packet=lambda packet: packet,
        inc=lambda _name, _amount: None,
        priority_pagination=1,
        enqueue_pagination_job=_capture_enqueue,
        logger=type("L", (), {"debug": lambda *args, **kwargs: None})(),
    )

    assert calls["executor"] == 1
    assert pkt_q.qsize() == 2


@pytest.mark.asyncio
async def test_parse_payload_emits_tagged_parse_metric() -> None:
    observed: list[tuple[str, float]] = []

    class _Loop:
        async def run_in_executor(self, executor: object, func: object) -> object:
            del executor
            return func()

    result = ParseResult(packets=[], next_jobs=[])

    @contextmanager
    def _span(*args: object, **kwargs: object) -> object:
        del args, kwargs
        yield

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("vertex_forager.core.workerio.asyncio.get_running_loop", lambda: _Loop())
        parsed = await parse_payload(
            job=_job("AAPL"),
            payload=b"ok",
            worker_id=1,
            parse_executor=object(),
            router_parse=lambda **kwargs: result,
            span=_span,
            observe=lambda name, value: observed.append((name, value)),
            log_structured=lambda **kwargs: None,
            logger=type("L", (), {"debug": lambda *args, **kwargs: None})(),
        )

    assert parsed is result
    names = [name for name, _ in observed]
    assert "parse_duration_s" in names
    assert "parse_duration_s.sharadar.price" in names


@pytest.mark.asyncio
async def test_record_worker_error_appends_run_error() -> None:
    result = RunResult(provider="sharadar")
    lock = asyncio.Lock()
    await record_worker_error(
        result=result,
        result_lock=lock,
        job=_job("AAPL"),
        exc=RuntimeError("boom"),
        worker_id=2,
        stage="error_unexpected",
        inc=lambda _name, _amount: None,
        log_structured=lambda **kwargs: None,
        logger=type(
            "L",
            (),
            {"exception": lambda *args, **kwargs: None, "error": lambda *args, **kwargs: None},
        )(),
    )
    assert len(result.errors) == 1
    assert result.errors[0].symbol == "AAPL"
