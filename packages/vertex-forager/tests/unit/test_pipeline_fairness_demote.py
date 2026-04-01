from __future__ import annotations

import asyncio

import pytest

from vertex_forager.core.config import FetchJob, RequestSpec, ResolvedClientConfig
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.core.scheduler import enqueue_pagination_job, mark_pagination_job_done


class _StubClient:
    async def aclose(self) -> None:  # pragma: no cover
        return None


@pytest.mark.asyncio
async def test_drr_handles_large_page_count_disparity_without_scan() -> None:
    config = ResolvedClientConfig(requests_per_minute=60, concurrency=1, pagination_max_burst=3)
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

    def job(sym: str, page: int) -> FetchJob:
        return FetchJob(
            provider="p",
            dataset="d",
            symbol=sym,
            spec=RequestSpec(url=f"http://example.com/{sym}/{page}"),
        )

    for page in range(1, 31):
        await enqueue_pagination_job(fair_lock=eng._fair_lock, fairness_state=eng._fair_state, job=job("A", page))
    await enqueue_pagination_job(fair_lock=eng._fair_lock, fairness_state=eng._fair_state, job=job("B", 1))

    first_round = []
    for _ in range(4):
        selected = await eng._dequeue_worker_job(req_q=q, burst_cap=3)  # type: ignore[attr-defined]
        assert selected.job is not None
        first_round.append(selected.job.symbol)
        await mark_pagination_job_done(fair_lock=eng._fair_lock, fairness_state=eng._fair_state)

    assert first_round == ["A", "A", "A", "B"]
