from __future__ import annotations

import asyncio
from collections import deque
import contextlib
from dataclasses import dataclass, field
import math
from typing import TYPE_CHECKING
from weakref import WeakValueDictionary

if TYPE_CHECKING:
    from vertex_forager.core.config import FetchJob


@dataclass
class FairnessState:
    """Pure mutable DRR pagination scheduling state."""

    queues: dict[str, deque[FetchJob]] = field(default_factory=dict)
    deficit: dict[str, float] = field(default_factory=dict)
    active: deque[str] = field(default_factory=deque)
    active_symbols: set[str] = field(default_factory=set)
    quantum: float = 3.0
    max_pending_per_symbol: int | None = None
    backpressure_threshold: int | None = None
    unfinished_jobs: int = 0
    total_backlog: int = 0

    def __post_init__(self) -> None:
        if not math.isfinite(self.quantum) or self.quantum <= 0:
            raise ValueError(f"quantum must be a finite positive number, got {self.quantum}")
        if self.max_pending_per_symbol is not None and self.max_pending_per_symbol <= 0:
            raise ValueError("max_pending_per_symbol must be positive when specified")
        if self.backpressure_threshold is not None and self.backpressure_threshold <= 0:
            raise ValueError("backpressure_threshold must be positive when specified")


@dataclass
class FairnessEvents:
    drained: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    available: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    below_threshold: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    symbol_capacity: dict[str, asyncio.Event] = field(default_factory=dict, repr=False)
    enqueue_ready: dict[str, asyncio.Event] = field(default_factory=dict, repr=False)

    def _ensure_symbol_capacity_event(self, symbol: str) -> asyncio.Event:
        event = self.symbol_capacity.get(symbol)
        if event is None:
            event = asyncio.Event()
            self.symbol_capacity[symbol] = event
        return event

    def _ensure_enqueue_ready_event(self, symbol: str) -> asyncio.Event:
        event = self.enqueue_ready.get(symbol)
        if event is None:
            event = asyncio.Event()
            self.enqueue_ready[symbol] = event
        return event

    def _cleanup_symbol_events(self, symbol: str) -> None:
        capacity_event = self.symbol_capacity.pop(symbol, None)
        if capacity_event is not None:
            capacity_event.set()
        enqueue_event = self.enqueue_ready.pop(symbol, None)
        if enqueue_event is not None:
            enqueue_event.set()

    def sync_symbol(self, *, fairness_state: FairnessState, symbol: str, depth: int) -> None:
        capacity_ok = (
            fairness_state.max_pending_per_symbol is None or depth < fairness_state.max_pending_per_symbol
        )
        threshold_ok = (
            fairness_state.backpressure_threshold is None
            or fairness_state.total_backlog < fairness_state.backpressure_threshold
        )
        if fairness_state.max_pending_per_symbol is None and depth == 0 and symbol not in fairness_state.queues:
            self._cleanup_symbol_events(symbol)
            return
        capacity_event = self._ensure_symbol_capacity_event(symbol)
        if capacity_ok:
            capacity_event.set()
        else:
            capacity_event.clear()
        enqueue_ready_event = self._ensure_enqueue_ready_event(symbol)
        if capacity_ok and threshold_ok:
            enqueue_ready_event.set()
        else:
            enqueue_ready_event.clear()

    def sync_from_state(self, *, fairness_state: FairnessState) -> None:
        if fairness_state.unfinished_jobs == 0:
            self.drained.set()
        else:
            self.drained.clear()
        if fairness_state.active:
            self.available.set()
        else:
            self.available.clear()
        threshold_ok = (
            fairness_state.backpressure_threshold is None
            or fairness_state.total_backlog < fairness_state.backpressure_threshold
        )
        if threshold_ok:
            self.below_threshold.set()
        else:
            self.below_threshold.clear()
        tracked_symbols = (
            set(fairness_state.queues)
            | set(self.symbol_capacity)
            | set(self.enqueue_ready)
            | set(fairness_state.active_symbols)
        )
        for symbol in tracked_symbols:
            depth = len(fairness_state.queues.get(symbol, ()))
            self.sync_symbol(fairness_state=fairness_state, symbol=symbol, depth=depth)

    def wait_event_for(self, *, fairness_state: FairnessState, symbol: str) -> asyncio.Event:
        self.sync_symbol(
            fairness_state=fairness_state,
            symbol=symbol,
            depth=len(fairness_state.queues.get(symbol, ())),
        )
        return self._ensure_enqueue_ready_event(symbol)


@dataclass(frozen=True)
class SchedulerResult:
    priority: int
    job: FetchJob | None
    demoted: list[FetchJob]
    already_done: bool


def _symbol_key(job: FetchJob) -> str:
    return job.symbol or ""


class FairnessWaiter:
    def __init__(
        self,
        *,
        fair_lock: asyncio.Lock,
        fairness_state: FairnessState,
        fairness_events: FairnessEvents,
    ) -> None:
        self._lock = fair_lock
        self._state = fairness_state
        self._events = fairness_events
        self._events.sync_from_state(fairness_state=self._state)

    def _try_enqueue_pagination_job(self, *, symbol: str, job: FetchJob) -> asyncio.Event | None:
        q = self._state.queues.get(symbol)
        current_depth = len(q) if q is not None else 0
        backlog_blocked = (
            self._state.backpressure_threshold is not None
            and self._state.total_backlog >= self._state.backpressure_threshold
        )
        symbol_blocked = (
            self._state.max_pending_per_symbol is not None
            and current_depth >= self._state.max_pending_per_symbol
        )
        if backlog_blocked or symbol_blocked:
            self._events.sync_from_state(fairness_state=self._state)
            return self._events.wait_event_for(fairness_state=self._state, symbol=symbol)
        if q is None:
            q = deque()
            self._state.queues[symbol] = q
        q.append(job)
        if symbol not in self._state.active_symbols:
            self._state.active.append(symbol)
            self._state.active_symbols.add(symbol)
        self._state.unfinished_jobs += 1
        self._state.total_backlog += 1
        self._events.sync_from_state(fairness_state=self._state)
        return None

    async def enqueue_pagination_job(self, *, job: FetchJob) -> None:
        symbol = _symbol_key(job)
        while True:
            async with self._lock:
                wait_event = self._try_enqueue_pagination_job(symbol=symbol, job=job)
                if wait_event is None:
                    return
                wait_task = asyncio.create_task(wait_event.wait())
            try:
                await wait_task
            except asyncio.CancelledError:
                wait_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await wait_task
                raise

    async def mark_pagination_job_done(self) -> None:
        async with self._lock:
            if self._state.unfinished_jobs <= 0:
                raise RuntimeError("pagination fairness accounting underflow")
            self._state.unfinished_jobs -= 1
            self._events.sync_from_state(fairness_state=self._state)

    async def wait_for_pagination_drain(self) -> None:
        self._events.sync_from_state(fairness_state=self._state)
        await self._events.drained.wait()

    async def wait_for_pagination_availability(self) -> None:
        self._events.sync_from_state(fairness_state=self._state)
        await self._events.available.wait()

    async def wait_for_pagination_below_threshold(self) -> None:
        self._events.sync_from_state(fairness_state=self._state)
        await self._events.below_threshold.wait()

    async def pop_next_job_respecting_fairness(self, *, priority_pagination: int) -> SchedulerResult:
        """Select the next paginated job using Deficit Round Robin."""
        async with self._lock:
            self._events.sync_from_state(fairness_state=self._state)
            while self._state.active:
                symbol = self._state.active[0]
                symbol_queue = self._state.queues.get(symbol)
                if not symbol_queue:
                    self._state.active.popleft()
                    self._state.active_symbols.discard(symbol)
                    self._state.queues.pop(symbol, None)
                    self._state.deficit.pop(symbol, None)
                    self._events._cleanup_symbol_events(symbol)
                    continue
                current_deficit = self._state.deficit.get(symbol, 0.0)
                if current_deficit < 1.0:
                    self._state.deficit[symbol] = current_deficit + self._state.quantum
                if self._state.deficit[symbol] < 1.0:
                    self._state.active.rotate(-1)
                    continue
                job = symbol_queue.popleft()
                self._state.total_backlog -= 1
                self._state.deficit[symbol] -= 1.0
                if symbol_queue:
                    self._events.sync_symbol(
                        fairness_state=self._state,
                        symbol=symbol,
                        depth=len(symbol_queue),
                    )
                    if self._state.deficit[symbol] < 1.0:
                        self._state.active.rotate(-1)
                else:
                    self._state.active.popleft()
                    self._state.active_symbols.discard(symbol)
                    self._state.queues.pop(symbol, None)
                    self._state.deficit.pop(symbol, None)
                    self._events._cleanup_symbol_events(symbol)
                self._events.sync_from_state(fairness_state=self._state)
                return SchedulerResult(priority=priority_pagination, job=job, demoted=[], already_done=False)
            self._events.sync_from_state(fairness_state=self._state)
            return SchedulerResult(priority=priority_pagination, job=None, demoted=[], already_done=False)


_WAITER_CACHE: WeakValueDictionary[tuple[int, int], FairnessWaiter] = WeakValueDictionary()


def _get_waiter(*, fair_lock: asyncio.Lock, fairness_state: FairnessState) -> FairnessWaiter:
    key = (id(fair_lock), id(fairness_state))
    waiter = _WAITER_CACHE.get(key)
    if waiter is None:
        waiter = FairnessWaiter(
            fair_lock=fair_lock,
            fairness_state=fairness_state,
            fairness_events=FairnessEvents(),
        )
        _WAITER_CACHE[key] = waiter
    return waiter


def _queued_backlog(*, fairness_state: FairnessState) -> int:
    return fairness_state.total_backlog


async def enqueue_pagination_job(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
    job: FetchJob,
) -> None:
    await _get_waiter(fair_lock=fair_lock, fairness_state=fairness_state).enqueue_pagination_job(job=job)


async def mark_pagination_job_done(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
) -> None:
    await _get_waiter(fair_lock=fair_lock, fairness_state=fairness_state).mark_pagination_job_done()


async def wait_for_pagination_drain(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
) -> None:
    await _get_waiter(
        fair_lock=fair_lock,
        fairness_state=fairness_state,
    ).wait_for_pagination_drain()


async def wait_for_pagination_availability(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
) -> None:
    await _get_waiter(
        fair_lock=fair_lock,
        fairness_state=fairness_state,
    ).wait_for_pagination_availability()


async def wait_for_pagination_below_threshold(
    *,
    fair_lock: asyncio.Lock,
    fairness_state: FairnessState,
) -> None:
    await _get_waiter(
        fair_lock=fair_lock,
        fairness_state=fairness_state,
    ).wait_for_pagination_below_threshold()


async def pop_next_job_respecting_fairness(
    *,
    fair_lock: asyncio.Lock,
    priority_pagination: int,
    fairness_state: FairnessState,
) -> SchedulerResult:
    return await _get_waiter(
        fair_lock=fair_lock,
        fairness_state=fairness_state,
    ).pop_next_job_respecting_fairness(priority_pagination=priority_pagination)
