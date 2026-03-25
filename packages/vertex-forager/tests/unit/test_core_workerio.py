from __future__ import annotations

import asyncio
from datetime import datetime
import itertools

import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, FramePacket, ParseResult, RequestSpec, RunResult
from vertex_forager.core.workerio import emit_packets_and_next_jobs, record_worker_error


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

    def _inc(name: str, amount: int) -> None:
        counters[name] = counters.get(name, 0) + amount

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
        logger=type("L", (), {"debug": lambda *args, **kwargs: None})(),
    )
    assert counters["packets_emitted"] == 1
    assert pkt_q.get_nowait().table == "sharadar_price"
    priority, _, next_job = req_q.get_nowait()
    assert priority == 1
    assert next_job is not None
    assert next_job.symbol == "MSFT"


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
