from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertex_forager.core.config import FetchJob


@dataclass
class FairnessState:
    """Mutable DRR pagination scheduling state.

    available is level-triggered state, not an edge-triggered wakeup signal.
    It stays set while any active symbol still has pending pagination work and
    is cleared only when the DRR lane becomes empty.
    """

    queues: dict[str, deque[FetchJob]] = field(default_factory=dict)
    deficit: dict[str, float] = field(default_factory=dict)
    active: deque[str] = field(default_factory=deque)
    active_symbols: set[str] = field(default_factory=set)
    quantum: float = 3.0
    unfinished_jobs: int = 0
    drained: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    available: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def __post_init__(self) -> None:
        if self.unfinished_jobs == 0:
            self.drained.set()
        if not math.isfinite(self.quantum) or self.quantum <= 0:
            raise ValueError(f"quantum must be a finite positive number, got {self.quantum}")


@dataclass(frozen=True)
class SchedulerResult:
    priority: int
    job: FetchJob | None
    demoted: list[FetchJob]
    already_done: bool


def _symbol_key(job: FetchJob) -> str:
    return job.symbol or ""


async def enqueue_pagination_job(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
    job: FetchJob,
) -> None:
    async with fair_lock:
        symbol = _symbol_key(job)
        q = fairness_state.queues.get(symbol)
        if q is None:
            q = deque()
            fairness_state.queues[symbol] = q
        q.append(job)
        if symbol not in fairness_state.active_symbols:
            fairness_state.active.append(symbol)
            fairness_state.active_symbols.add(symbol)
        fairness_state.unfinished_jobs += 1
        fairness_state.drained.clear()
        fairness_state.available.set()


async def mark_pagination_job_done(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
) -> None:
    async with fair_lock:
        if fairness_state.unfinished_jobs <= 0:
            raise RuntimeError("pagination fairness accounting underflow")
        fairness_state.unfinished_jobs -= 1
        if fairness_state.unfinished_jobs == 0:
            fairness_state.drained.set()


async def wait_for_pagination_drain(*, fairness_state: FairnessState) -> None:
    await fairness_state.drained.wait()


async def wait_for_pagination_availability(*, fairness_state: FairnessState) -> None:
    await fairness_state.available.wait()


async def pop_next_job_respecting_fairness(
    *,
    fair_lock: asyncio.Lock,
    priority_pagination: int,
    fairness_state: FairnessState,
) -> SchedulerResult:
    """Select the next paginated job using Deficit Round Robin."""
    async with fair_lock:
        while fairness_state.active:
            symbol = fairness_state.active[0]
            symbol_queue = fairness_state.queues.get(symbol)
            if not symbol_queue:
                fairness_state.active.popleft()
                fairness_state.active_symbols.discard(symbol)
                fairness_state.queues.pop(symbol, None)
                fairness_state.deficit.pop(symbol, None)
                continue
            if fairness_state.deficit.get(symbol, 0.0) < 1.0:
                fairness_state.deficit[symbol] = fairness_state.deficit.get(symbol, 0.0) + fairness_state.quantum
            if fairness_state.deficit[symbol] < 1.0:
                fairness_state.active.rotate(-1)
                continue
            job = symbol_queue.popleft()
            fairness_state.deficit[symbol] -= 1.0
            if symbol_queue:
                if fairness_state.deficit[symbol] < 1.0:
                    fairness_state.active.rotate(-1)
            else:
                fairness_state.active.popleft()
                fairness_state.active_symbols.discard(symbol)
                fairness_state.queues.pop(symbol, None)
                fairness_state.deficit.pop(symbol, None)
            if not fairness_state.active:
                fairness_state.available.clear()
            return SchedulerResult(priority=priority_pagination, job=job, demoted=[], already_done=False)
        fairness_state.available.clear()
        return SchedulerResult(priority=priority_pagination, job=None, demoted=[], already_done=False)
