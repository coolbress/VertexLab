from __future__ import annotations

import asyncio

import pytest

from vertex_forager.core.config import FetchJob, RequestSpec
from vertex_forager.core.scheduler import FairnessState, pop_next_job_respecting_fairness


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
    state = FairnessState()
    result = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=2,
        priority_pagination=1,
        priority_new_job=0,
        fairness_state=state,
    )
    assert result.priority == 99
    assert result.job is None
    assert result.demoted == []
    assert result.already_done is True
    assert state.last_symbol is None
    assert state.burst_count == 0


@pytest.mark.asyncio
async def test_pop_next_job_respecting_fairness_demotes_excess_burst() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    req_q.put_nowait((1, 0, _job("AAPL")))
    req_q.put_nowait((1, 1, _job("AAPL")))
    req_q.put_nowait((2, 2, _job("MSFT")))
    state = FairnessState(last_symbol="AAPL", burst_count=2)
    result = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=2,
        priority_pagination=1,
        priority_new_job=0,
        fairness_state=state,
    )
    assert result.priority == 2
    assert result.job is not None
    assert result.job.symbol == "MSFT"
    assert [d.symbol for d in result.demoted] == ["AAPL", "AAPL"]
    assert result.already_done is False
    assert state.last_symbol is None
    assert state.burst_count == 0


@pytest.mark.asyncio
async def test_pop_next_job_symbol_change_resets_burst_to_one() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    req_q.put_nowait((1, 0, _job("MSFT")))
    state = FairnessState(last_symbol="AAPL", burst_count=3)
    result = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=10,
        priority_pagination=1,
        priority_new_job=0,
        fairness_state=state,
    )
    assert result.job is not None
    assert result.job.symbol == "MSFT"
    assert state.last_symbol == "MSFT"
    assert state.burst_count == 1


@pytest.mark.asyncio
async def test_pop_next_job_empty_queue_after_demotion_returns_no_candidate() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    req_q.put_nowait((1, 0, _job("AAPL")))
    req_q.put_nowait((1, 1, _job("AAPL")))
    state = FairnessState(last_symbol="AAPL", burst_count=2)
    result = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=2,
        priority_pagination=1,
        priority_new_job=0,
        fairness_state=state,
    )
    assert result.priority == 0
    assert result.job is None
    assert [d.symbol for d in result.demoted] == ["AAPL", "AAPL"]
    assert result.already_done is False


@pytest.mark.asyncio
async def test_pop_next_job_mutates_fairness_state_in_place() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    req_q.put_nowait((1, 0, _job("TSLA")))
    state = FairnessState(last_symbol=None, burst_count=0)
    original_id = id(state)
    _ = await pop_next_job_respecting_fairness(
        req_q=req_q,
        fair_lock=asyncio.Lock(),
        burst_cap=3,
        priority_pagination=1,
        priority_new_job=0,
        fairness_state=state,
    )
    assert id(state) == original_id
    assert state.last_symbol == "TSLA"
    assert state.burst_count == 1
