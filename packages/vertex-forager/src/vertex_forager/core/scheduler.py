from __future__ import annotations

import asyncio
from collections import deque
import contextlib
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
    max_pending_per_symbol: int | None = None
    backpressure_threshold: int | None = None
    unfinished_jobs: int = 0
    drained: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    available: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    below_threshold: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    symbol_capacity: dict[str, asyncio.Event] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.unfinished_jobs == 0:
            self.drained.set()
        if not math.isfinite(self.quantum) or self.quantum <= 0:
            raise ValueError(f"quantum must be a finite positive number, got {self.quantum}")
        if self.max_pending_per_symbol is not None and self.max_pending_per_symbol <= 0:
            raise ValueError("max_pending_per_symbol must be positive when specified")
        if self.backpressure_threshold is not None and self.backpressure_threshold <= 0:
            raise ValueError("backpressure_threshold must be positive when specified")
        if self.backpressure_threshold is None or self.unfinished_jobs < self.backpressure_threshold:
            self.below_threshold.set()


@dataclass(frozen=True)
class SchedulerResult:
    priority: int
    job: FetchJob | None
    demoted: list[FetchJob]
    already_done: bool


def _symbol_key(job: FetchJob) -> str:
    return job.symbol or ""


def _ensure_symbol_capacity_event(*, fairness_state: FairnessState, symbol: str) -> asyncio.Event:
    event = fairness_state.symbol_capacity.get(symbol)
    if event is None:
        event = asyncio.Event()
        fairness_state.symbol_capacity[symbol] = event
    return event


def _sync_below_threshold_event(*, fairness_state: FairnessState) -> None:
    if (
        fairness_state.backpressure_threshold is None
        or fairness_state.unfinished_jobs < fairness_state.backpressure_threshold
    ):
        fairness_state.below_threshold.set()
    else:
        fairness_state.below_threshold.clear()


def _sync_symbol_capacity_event(*, fairness_state: FairnessState, symbol: str, depth: int) -> None:
    if fairness_state.max_pending_per_symbol is None:
        return
    event = _ensure_symbol_capacity_event(fairness_state=fairness_state, symbol=symbol)
    if depth >= fairness_state.max_pending_per_symbol:
        event.clear()
    else:
        event.set()


def _try_enqueue_pagination_job(*, fairness_state: FairnessState, symbol: str, job: FetchJob) -> list[asyncio.Event]:
    q = fairness_state.queues.get(symbol)
    current_depth = len(q) if q is not None else 0
    waits: list[asyncio.Event] = []
    if (
        fairness_state.backpressure_threshold is not None
        and fairness_state.unfinished_jobs >= fairness_state.backpressure_threshold
    ):
        waits.append(fairness_state.below_threshold)
    if fairness_state.max_pending_per_symbol is not None and current_depth >= fairness_state.max_pending_per_symbol:
        waits.append(_ensure_symbol_capacity_event(fairness_state=fairness_state, symbol=symbol))
    if waits:
        return waits
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
    _sync_below_threshold_event(fairness_state=fairness_state)
    _sync_symbol_capacity_event(fairness_state=fairness_state, symbol=symbol, depth=len(q))
    return []


async def enqueue_pagination_job(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
    job: FetchJob,
) -> None:
    symbol = _symbol_key(job)
    while True:
        async with fair_lock:
            wait_events = _try_enqueue_pagination_job(fairness_state=fairness_state, symbol=symbol, job=job)
            if not wait_events:
                return
            waits = [asyncio.create_task(event.wait()) for event in wait_events]
        done, pending = await asyncio.wait(set(waits), return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            task.result()


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
        _sync_below_threshold_event(fairness_state=fairness_state)


async def wait_for_pagination_drain(*, fairness_state: FairnessState) -> None:
    await fairness_state.drained.wait()


async def wait_for_pagination_availability(*, fairness_state: FairnessState) -> None:
    await fairness_state.available.wait()


async def wait_for_pagination_below_threshold(*, fairness_state: FairnessState) -> None:
    await fairness_state.below_threshold.wait()


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
            current_deficit = fairness_state.deficit.get(symbol, 0.0)
            if current_deficit < 1.0:
                fairness_state.deficit[symbol] = current_deficit + fairness_state.quantum
            if fairness_state.deficit[symbol] < 1.0:
                fairness_state.active.rotate(-1)
                continue
            job = symbol_queue.popleft()
            fairness_state.deficit[symbol] -= 1.0
            _sync_symbol_capacity_event(fairness_state=fairness_state, symbol=symbol, depth=len(symbol_queue))
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
