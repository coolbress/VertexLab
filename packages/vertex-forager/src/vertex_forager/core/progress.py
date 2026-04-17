from __future__ import annotations

from collections import deque
from collections.abc import Callable
import inspect
import os
import time
from typing import Any, cast

import psutil
from tqdm import tqdm

from vertex_forager.core.config import ProgressSnapshot

PROGRESS_EMIT_INTERVAL_S = 0.1


def compute_window_start(
    *, progress_started_at: float, progress_history: deque[tuple[float, int]], now: float
) -> float:
    if len(progress_history) == 1:
        return progress_started_at
    if progress_history:
        return progress_history[0][0]
    return now


def format_progress_summary(*, provider: str, dataset: str, snapshot: ProgressSnapshot) -> str:
    pct = f"{snapshot.pct:.1f}%" if snapshot.pct is not None else "n/a"
    eta = f"{snapshot.eta_s:.1f}s" if snapshot.eta_s is not None else "n/a"
    return (
        f"vertex-forager run complete | provider={provider} dataset={dataset} jobs={snapshot.jobs_done}"
        f"/{snapshot.jobs_total if snapshot.jobs_total is not None else '?'} pct={pct} "
        f"elapsed={snapshot.elapsed_s:.1f}s throughput={snapshot.throughput_sym_per_s:.2f}/s "
        f"eta={eta} rows={snapshot.rows_written} errors={snapshot.errors} retries={snapshot.retries} "
        f"throttle_events={snapshot.throttle_events} dlq_spooled={snapshot.dlq_spooled}"
    )


class ProgressEmitter:
    """Encapsulate progress snapshot accounting and emission."""

    def __init__(self) -> None:
        self._started_at = 0.0
        self._jobs_total: int | None = None
        self._jobs_done = 0
        self._history: deque[tuple[float, int]] = deque()
        self._window_events = 0
        self._active_workers = 0
        self._display_done = 0
        self._counters: dict[str, int] = {}
        self._process: psutil.Process | None = None
        self._last_emit_at = 0.0

    def reset(self, *, jobs_total: int | None, jobs_done_initial: int = 0) -> None:
        self._started_at = time.monotonic()
        self._jobs_total = jobs_total
        self._jobs_done = jobs_done_initial
        self._history.clear()
        self._window_events = 0
        self._active_workers = 0
        self._display_done = jobs_done_initial
        self._counters = {}
        self._process = None
        self._last_emit_at = 0.0

    def _ensure_process_initialized(self) -> psutil.Process:
        if self._process is None:
            self._process = psutil.Process(os.getpid())
            self._process.cpu_percent(interval=None)
        return self._process

    def inc_counter(self, name: str, amount: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + amount

    def worker_started(self) -> None:
        self._active_workers += 1

    def worker_finished(self) -> None:
        self._active_workers = max(0, self._active_workers - 1)

    def record_terminal(self, count: int) -> None:
        if count <= 0:
            return
        now = time.monotonic()
        self._jobs_done += count
        self._history.append((now, count))
        self._window_events += count
        cutoff = now - 30.0
        while self._history and self._history[0][0] < cutoff:
            _ts, removed = self._history.popleft()
            self._window_events = max(0, self._window_events - removed)

    def build_snapshot(
        self,
        *,
        pending_jobs: int,
        errors: int,
        retries: int,
        throttle_events: int,
        finished: bool,
        result_duration_s: float | None = None,
    ) -> ProgressSnapshot:
        process = self._ensure_process_initialized()
        now = time.monotonic()
        cutoff = now - 30.0
        while self._history and self._history[0][0] < cutoff:
            _ts, removed = self._history.popleft()
            self._window_events = max(0, self._window_events - removed)
        elapsed_s = max(0.0, now - self._started_at)
        window_start = compute_window_start(
            progress_started_at=self._started_at,
            progress_history=self._history,
            now=now,
        )
        window_span = min(30.0, max(0.0, now - window_start)) if self._history else 0.0
        throughput = float(self._window_events / window_span) if window_span > 0.0 else 0.0
        if self._jobs_total not in (None, 0):
            known_jobs_total = cast(int, self._jobs_total)
            pct = float((self._jobs_done / known_jobs_total) * 100.0)
            eta_s = float(max(0, known_jobs_total - self._jobs_done) / throughput) if throughput > 0.0 else None
        else:
            pct = None
            eta_s = None
        return ProgressSnapshot(
            jobs_done=self._jobs_done,
            jobs_total=self._jobs_total,
            pct=pct,
            throughput_sym_per_s=throughput,
            eta_s=eta_s,
            errors=errors,
            retries=retries,
            rows_written=int(self._counters.get("rows_written_total", 0)),
            elapsed_s=float(result_duration_s) if finished and result_duration_s is not None else elapsed_s,
            active_workers=self._active_workers,
            pending_jobs=pending_jobs,
            throttle_events=throttle_events,
            dlq_spooled=int(self._counters.get("dlq_spooled_files_total", 0)),
            memory_mb=float(process.memory_info().rss / (1024 * 1024)),
            cpu_pct=float(process.cpu_percent(interval=None)),
            finished=finished,
        )

    async def emit(
        self,
        *,
        snapshot: ProgressSnapshot,
        on_progress: Callable[[ProgressSnapshot], Any] | None,
        progress_bar: tqdm | None,
        show_summary: bool,
        provider: str,
        dataset: str,
        logger: Any,
    ) -> None:
        now = time.monotonic()
        if not snapshot.finished and self._last_emit_at and now - self._last_emit_at < PROGRESS_EMIT_INTERVAL_S:
            return
        self._last_emit_at = now
        if progress_bar is not None:
            delta = max(0, snapshot.jobs_done - self._display_done)
            if delta:
                progress_bar.update(delta)
                self._display_done += delta
            progress_bar.set_postfix(
                throughput=f"{snapshot.throughput_sym_per_s:.2f}/s",
                eta="n/a" if snapshot.eta_s is None else f"{snapshot.eta_s:.1f}s",
                errors=snapshot.errors,
                rows=snapshot.rows_written,
                refresh=False,
            )
            if snapshot.finished:
                progress_bar.close()
        if on_progress is None:
            if snapshot.finished and show_summary:
                print(format_progress_summary(provider=provider, dataset=dataset, snapshot=snapshot))
            return
        try:
            maybe_awaitable = on_progress(snapshot)
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception as exc:
            logger.error("Error in on_progress callback: %s", exc)
        if snapshot.finished and show_summary:
            print(format_progress_summary(provider=provider, dataset=dataset, snapshot=snapshot))


__all__ = ["ProgressEmitter", "compute_window_start", "format_progress_summary"]
