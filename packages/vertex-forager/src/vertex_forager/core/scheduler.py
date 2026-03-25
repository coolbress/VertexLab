from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertex_forager.core.config import FetchJob


async def pop_next_job_respecting_fairness(
    *,
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    fair_lock: asyncio.Lock,
    burst_cap: int,
    priority_pagination: int,
    priority_new_job: int,
    fair_last_symbol: str | None,
    fair_burst_count: int,
) -> tuple[int, FetchJob | None, list[FetchJob], bool, str | None, int]:
    demote_jobs: list[FetchJob] = []
    already_done = False
    async with fair_lock:
        while True:
            priority, _, job = await req_q.get()
            if job is None:
                req_q.task_done()
                return priority, None, demote_jobs, True, fair_last_symbol, fair_burst_count
            if priority != priority_pagination:
                return priority, job, demote_jobs, already_done, None, 0
            if fair_last_symbol == job.symbol:
                fair_burst_count += 1
            else:
                fair_last_symbol = job.symbol
                fair_burst_count = 1
            if fair_burst_count <= burst_cap:
                return priority, job, demote_jobs, already_done, fair_last_symbol, fair_burst_count
            demote_jobs.append(job)
            return _pick_after_demotion(
                req_q=req_q,
                demote_jobs=demote_jobs,
                already_done=already_done,
                priority_pagination=priority_pagination,
                priority_new_job=priority_new_job,
                fair_last_symbol=fair_last_symbol,
                fair_burst_count=fair_burst_count,
            )


def _pick_after_demotion(
    *,
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    demote_jobs: list[FetchJob],
    already_done: bool,
    priority_pagination: int,
    priority_new_job: int,
    fair_last_symbol: str | None,
    fair_burst_count: int,
) -> tuple[int, FetchJob | None, list[FetchJob], bool, str | None, int]:
    while True:
        try:
            p2, _, cand = req_q.get_nowait()
        except asyncio.QueueEmpty:
            return priority_new_job, None, demote_jobs, already_done, fair_last_symbol, fair_burst_count
        if p2 == priority_pagination and cand is not None and cand.symbol == fair_last_symbol:
            demote_jobs.append(cand)
            continue
        if cand is None:
            req_q.task_done()
            return p2, None, demote_jobs, True, fair_last_symbol, fair_burst_count
        if p2 == priority_pagination:
            fair_last_symbol = cand.symbol
            fair_burst_count = 1
        else:
            fair_last_symbol = None
            fair_burst_count = 0
        return p2, cand, demote_jobs, already_done, fair_last_symbol, fair_burst_count
