from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertex_forager.core.config import FetchJob


@dataclass
class FairnessState:
    """Mutable pagination fairness state.

    last_symbol stores the symbol selected most recently from the pagination
    priority lane. burst_count stores how many consecutive selections were made
    for last_symbol from that same lane.
    """

    last_symbol: str | None = None
    burst_count: int = 0


@dataclass(frozen=True)
class SchedulerResult:
    priority: int
    job: FetchJob | None
    demoted: list[FetchJob]
    already_done: bool


async def pop_next_job_respecting_fairness(
    *,
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    fair_lock: asyncio.Lock,
    burst_cap: int,
    priority_pagination: int,
    priority_new_job: int,
    fairness_state: FairnessState,
) -> SchedulerResult:
    """Select the next queue item while enforcing pagination burst fairness.

    Args:
        req_q: PriorityQueue of (priority:int, seq:int, FetchJob|None). None is
            treated as a sentinel.
        fair_lock: Async lock guarding fair state updates and fairness dequeue
            logic.
        burst_cap: Maximum consecutive picks allowed for the same symbol from
            the pagination priority lane.
        priority_pagination: Priority value used for paginated follow-up jobs.
        priority_new_job: Priority value used when demoted jobs should be
            requeued as normal jobs.
        fairness_state: Shared mutable fairness state, updated in-place under
            fair_lock.

    Returns:
        SchedulerResult:
        - priority: selected priority
        - job: selected FetchJob or None (sentinel/defer path)
        - demoted: jobs caller should requeue at priority_new_job
        - already_done: True when task_done was already called for selected
          sentinel

    Side effects:
        - Reads/removes items from req_q.
        - Updates fairness_state in-place.
        - Calls req_q.task_done() for consumed sentinels.
    """
    demote_jobs: list[FetchJob] = []
    already_done = False
    state = fairness_state
    async with fair_lock:
        priority, _, job = await req_q.get()
        if job is None:
            req_q.task_done()
            return SchedulerResult(priority=priority, job=None, demoted=demote_jobs, already_done=True)
        if priority != priority_pagination:
            state.last_symbol = None
            state.burst_count = 0
            return SchedulerResult(priority=priority, job=job, demoted=demote_jobs, already_done=already_done)
        if state.last_symbol == job.symbol:
            state.burst_count += 1
        else:
            state.last_symbol = job.symbol
            state.burst_count = 1
        if state.burst_count <= burst_cap:
            return SchedulerResult(priority=priority, job=job, demoted=demote_jobs, already_done=already_done)
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
) -> SchedulerResult:
    # O(N) over currently queued items in the worst case when many consecutive
    # pagination jobs for the same symbol are demoted in a single selection pass.
    while True:
        try:
            p2, order2, cand = req_q.get_nowait()
        except asyncio.QueueEmpty:
            return SchedulerResult(
                priority=priority_new_job,
                job=None,
                demoted=demote_jobs,
                already_done=already_done,
            )
        if p2 == priority_pagination and cand is not None and cand.symbol == fairness_state.last_symbol:
            # Do not call req_q.task_done() here: demoted jobs are acknowledged after requeue in
            # pipeline._requeue_demoted_jobs, which owns the task_done lifecycle for demotions.
            demote_jobs.append(cand)
            continue
        if cand is None:
            if demote_jobs:
                req_q.task_done()
                req_q.put_nowait((p2, order2, None))
                return SchedulerResult(
                    priority=priority_new_job,
                    job=None,
                    demoted=demote_jobs,
                    already_done=already_done,
                )
            req_q.task_done()
            return SchedulerResult(priority=p2, job=None, demoted=demote_jobs, already_done=True)
        if p2 == priority_pagination:
            fairness_state.last_symbol = cand.symbol
            fairness_state.burst_count = 1
        else:
            fairness_state.last_symbol = None
            fairness_state.burst_count = 0
        return SchedulerResult(priority=p2, job=cand, demoted=demote_jobs, already_done=already_done)
