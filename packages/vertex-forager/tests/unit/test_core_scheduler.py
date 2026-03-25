from __future__ import annotations

import asyncio

import pytest

from vertex_forager.core.config import FetchJob, RequestSpec
from vertex_forager.core.scheduler import pop_next_job_respecting_fairness


def _job(symbol: str) -> FetchJob:
    return FetchJob(
        provider="sharadar",
        dataset="price",
        symbol=symbol,
        spec=RequestSpec(url=f"https://example.test/{symbol}", params={"ticker": symbol}),
    )


@pytest.mark.asyncio
async def test_pop_next_job_respecting_fairness_consumes_sentinel() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    req_q.put_nowait((99, 0, None))
    priority, job, demotes, done, last_symbol, burst_count = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=2,
        priority_pagination=1,
        priority_new_job=0,
        fair_last_symbol=None,
        fair_burst_count=0,
    )
    assert priority == 99
    assert job is None
    assert demotes == []
    assert done is True
    assert last_symbol is None
    assert burst_count == 0


@pytest.mark.asyncio
async def test_pop_next_job_respecting_fairness_demotes_excess_burst() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    req_q.put_nowait((1, 0, _job("AAPL")))
    req_q.put_nowait((1, 1, _job("AAPL")))
    req_q.put_nowait((2, 2, _job("MSFT")))
    priority, job, demotes, done, last_symbol, burst_count = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=2,
        priority_pagination=1,
        priority_new_job=0,
        fair_last_symbol="AAPL",
        fair_burst_count=2,
    )
    assert priority == 2
    assert job is not None
    assert job.symbol == "MSFT"
    assert [d.symbol for d in demotes] == ["AAPL", "AAPL"]
    assert done is False
    assert last_symbol is None
    assert burst_count == 0
