from __future__ import annotations

import asyncio
import itertools

import pytest

from vertex_forager.core.config import EngineConfig, FetchJob, RequestSpec
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager


class _StubClient:
    async def aclose(self) -> None:  # pragma: no cover
        return None


@pytest.mark.asyncio
async def test_pop_next_job_respecting_fairness_demotes_excess_burst() -> None:
    config = EngineConfig(requests_per_minute=60, concurrency=1)
    controller = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=None,  # type: ignore[arg-type]
        mapper=None,
        config=config,
        controller=controller,
    )
    q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue()
    counter = itertools.count()

    def job(sym: str) -> FetchJob:
        return FetchJob(provider="p", dataset="d", symbol=sym, spec=RequestSpec(url="http://example.com"))

    # Queue: pagination A, pagination A, pagination B
    await q.put((eng.PRIORITY_PAGINATION, next(counter), job("A")))
    await q.put((eng.PRIORITY_PAGINATION, next(counter), job("A")))
    await q.put((eng.PRIORITY_PAGINATION, next(counter), job("B")))

    # With burst_cap=1, picking should demote the second A and return first A,
    # then on next pick it should return B (since A burst exceeded)
    selected = await eng._dequeue_worker_job(req_q=q, burst_cap=1)  # type: ignore[attr-defined]
    assert selected.job is not None
    assert selected.job.symbol == "A"
    # Requeue demotes at NEW_JOB priority
    for d in selected.demoted:
        await q.put((eng.PRIORITY_NEW_JOB, next(counter), d))
    # Next pick
    selected2 = await eng._dequeue_worker_job(req_q=q, burst_cap=1)  # type: ignore[attr-defined]
    assert selected2.job is not None
    assert selected2.job.symbol in {"A", "B"}
    # If fairness enforced, B should appear before the demoted A
    assert selected2.job.symbol == "B"
