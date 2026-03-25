from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertex_forager.core.config import FetchJob


@dataclass
class FairnessState:
    last_symbol: str | None = None
    burst_count: int = 0


async def pop_next_job_respecting_fairness(
    *,
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    fair_lock: asyncio.Lock,
    burst_cap: int,
    priority_pagination: int,
    priority_new_job: int,
    fairness_state: FairnessState | None = None,
    fair_last_symbol: str | None = None,
    fair_burst_count: int = 0,
) -> tuple[int, FetchJob | None, list[FetchJob], bool, str | None, int]:
    demote_jobs: list[FetchJob] = []
    already_done = False
    state = fairness_state or FairnessState(last_symbol=fair_last_symbol, burst_count=fair_burst_count)
    if fairness_state is not None:
        state.last_symbol = fair_last_symbol
        state.burst_count = fair_burst_count
    async with fair_lock:
        priority, _, job = await req_q.get()
        if job is None:
            req_q.task_done()
            return priority, None, demote_jobs, True, state.last_symbol, state.burst_count
        if priority != priority_pagination:
            state.last_symbol = None
            state.burst_count = 0
            return priority, job, demote_jobs, already_done, state.last_symbol, state.burst_count
        if state.last_symbol == job.symbol:
            state.burst_count += 1
        else:
            state.last_symbol = job.symbol
            state.burst_count = 1
        if state.burst_count <= burst_cap:
            return priority, job, demote_jobs, already_done, state.last_symbol, state.burst_count
        demote_jobs.append(job)
        return _pick_after_demotion(
            req_q=req_q,
            demote_jobs=demote_jobs,
            already_done=already_done,
            priority_pagination=priority_pagination,
            priority_new_job=priority_new_job,
            fairness_state=state,
        )


def _pick_after_demotion(
    *,
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    demote_jobs: list[FetchJob],
    already_done: bool,
    priority_pagination: int,
    priority_new_job: int,
    fairness_state: FairnessState,
) -> tuple[int, FetchJob | None, list[FetchJob], bool, str | None, int]:
    while True:
        try:
            p2, order2, cand = req_q.get_nowait()
        except asyncio.QueueEmpty:
            return (
                priority_new_job,
                None,
                demote_jobs,
                already_done,
                fairness_state.last_symbol,
                fairness_state.burst_count,
            )
        if p2 == priority_pagination and cand is not None and cand.symbol == fairness_state.last_symbol:
            demote_jobs.append(cand)
            continue
        if cand is None:
            if demote_jobs:
                req_q.task_done()
                req_q.put_nowait((p2, order2, None))
                return (
                    priority_new_job,
                    None,
                    demote_jobs,
                    already_done,
                    fairness_state.last_symbol,
                    fairness_state.burst_count,
                )
            req_q.task_done()
            return p2, None, demote_jobs, True, fairness_state.last_symbol, fairness_state.burst_count
        if p2 == priority_pagination:
            fairness_state.last_symbol = cand.symbol
            fairness_state.burst_count = 1
        else:
            fairness_state.last_symbol = None
            fairness_state.burst_count = 0
        return p2, cand, demote_jobs, already_done, fairness_state.last_symbol, fairness_state.burst_count
