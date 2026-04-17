from __future__ import annotations

import asyncio

import pytest

from vertex_forager.core.config import RequestSpec
from vertex_forager.core.domain import FetchJob
from vertex_forager.core.scheduler import FairnessEvents, FairnessState, FairnessWaiter

pytestmark = pytest.mark.manual


def _job(symbol: str, page: int) -> FetchJob:
    return FetchJob(
        provider="test",
        dataset="price",
        symbol=symbol,
        spec=RequestSpec(url=f"https://example.test/{symbol}/{page}", params={"ticker": symbol, "page": page}),
    )


def _assert_total_backlog_matches_queues(state: FairnessState) -> None:
    assert state.total_backlog == sum(len(queue) for queue in state.queues.values())


@pytest.mark.asyncio
async def test_scheduler_fairness_behavior_under_load() -> None:
    state = FairnessState(quantum=1.0)
    waiter = FairnessWaiter(
        fair_lock=asyncio.Lock(),
        fairness_state=state,
        fairness_events=FairnessEvents(),
    )

    symbols = [f"S{i:02d}" for i in range(20)]
    for page in range(5):
        for symbol in symbols:
            await waiter.enqueue_pagination_job(job=_job(symbol, page))
            _assert_total_backlog_matches_queues(state)

    seen = []
    for _ in range(20):
        selected = await waiter.pop_next_job_respecting_fairness(priority_pagination=1)
        assert selected.job is not None
        seen.append(selected.job.symbol)
        _assert_total_backlog_matches_queues(state)
    assert seen == symbols

    backpressure_state = FairnessState(quantum=1.0, backpressure_threshold=2)
    backpressure_waiter = FairnessWaiter(
        fair_lock=asyncio.Lock(),
        fairness_state=backpressure_state,
        fairness_events=FairnessEvents(),
    )
    await backpressure_waiter.enqueue_pagination_job(job=_job("AAPL", 1))
    assert backpressure_waiter._events.below_threshold.is_set() is True
    await backpressure_waiter.enqueue_pagination_job(job=_job("MSFT", 1))
    assert backpressure_waiter._events.below_threshold.is_set() is False
    selected = await backpressure_waiter.pop_next_job_respecting_fairness(priority_pagination=1)
    assert selected.job is not None
    assert backpressure_waiter._events.below_threshold.is_set() is True
    _assert_total_backlog_matches_queues(backpressure_state)
