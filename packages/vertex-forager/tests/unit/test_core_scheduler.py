from __future__ import annotations

import asyncio

import pytest

from vertex_forager.core.config import FetchJob, RequestSpec
from vertex_forager.core.scheduler import (
    FairnessState,
    enqueue_pagination_job,
    mark_pagination_job_done,
    pop_next_job_respecting_fairness,
)


def _job(symbol: str, page: int) -> FetchJob:
    return FetchJob(
        provider="sharadar",
        dataset="price",
        symbol=symbol,
        spec=RequestSpec(url=f"https://example.test/{symbol}/{page}", params={"ticker": symbol, "page": page}),
    )


@pytest.mark.asyncio
async def test_drr_rotates_symbols_after_quantum() -> None:
    state = FairnessState(quantum=3)
    lock = asyncio.Lock()
    for page in range(1, 5):
        await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("AAPL", page))
        await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("MSFT", page))

    seen: list[str] = []
    for _ in range(8):
        result = await pop_next_job_respecting_fairness(
            fair_lock=lock,
            priority_pagination=1,
            fairness_state=state,
        )
        assert result.job is not None
        seen.append(result.job.symbol or "")
        await mark_pagination_job_done(fair_lock=lock, fairness_state=state)

    assert seen == ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT", "MSFT", "AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_single_symbol_finishes_without_free_credit_carryover() -> None:
    state = FairnessState(quantum=3)
    lock = asyncio.Lock()
    await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("AAPL", 1))
    await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("AAPL", 2))

    first = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )
    second = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )
    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)
    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)

    assert first.job is not None
    assert first.job.symbol == "AAPL"
    assert second.job is not None
    assert second.job.symbol == "AAPL"
    assert state.deficit.get("AAPL", 0.0) == 0.0
    assert list(state.active) == []


@pytest.mark.asyncio
async def test_new_symbol_enters_active_list_mid_run() -> None:
    state = FairnessState(quantum=3)
    lock = asyncio.Lock()
    for page in range(1, 6):
        await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("AAPL", page))

    first = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )
    second = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )
    assert first.job is not None
    assert first.job.symbol == "AAPL"
    assert second.job is not None
    assert second.job.symbol == "AAPL"
    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)
    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)

    await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("MSFT", 1))

    third = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )
    fourth = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )
    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)
    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)

    assert third.job is not None
    assert third.job.symbol == "AAPL"
    assert fourth.job is not None
    assert fourth.job.symbol == "MSFT"


@pytest.mark.asyncio
async def test_drr_drained_event_tracks_unfinished_jobs() -> None:
    state = FairnessState(quantum=3)
    lock = asyncio.Lock()
    await enqueue_pagination_job(fair_lock=lock, fairness_state=state, job=_job("TSLA", 1))

    assert state.drained.is_set() is False

    result = await pop_next_job_respecting_fairness(
        fair_lock=lock,
        priority_pagination=1,
        fairness_state=state,
    )

    assert result.job is not None
    assert result.job.symbol == "TSLA"

    await mark_pagination_job_done(fair_lock=lock, fairness_state=state)

    assert state.drained.is_set() is True
