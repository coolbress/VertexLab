from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from contextlib import suppress
import sqlite3
import threading
from typing import Literal, Protocol

from vertex_forager.core.checkpoint import (
    Checkpoint,
    delete_checkpoints,
    find_latest_checkpoint,
    open_state_db,
    save_checkpoint,
)
from vertex_forager.core.domain import FetchJob
from vertex_forager.writers.memory import InMemoryBufferWriter


class PendingJobRegistry:
    """Track pending jobs with O(1) dedup and membership semantics."""

    def __init__(self, jobs: Sequence[FetchJob] = ()) -> None:
        self._jobs: dict[FetchJob, None] = {}
        self.add(jobs)

    def add(self, jobs: Sequence[FetchJob]) -> None:
        for job in jobs:
            self._jobs.setdefault(job, None)

    def remove(self, job: FetchJob) -> None:
        self._jobs.pop(job, None)

    def clear(self) -> None:
        self._jobs.clear()

    def snapshot(self, extra_jobs: Sequence[FetchJob] = ()) -> list[FetchJob]:
        jobs = list(self._jobs)
        seen = dict(self._jobs)
        for job in extra_jobs:
            if job in seen:
                continue
            seen[job] = None
            jobs.append(job)
        return jobs

    def __contains__(self, job: object) -> bool:
        return job in self._jobs

    def __len__(self) -> int:
        return len(self._jobs)


class _LoggerLike(Protocol):
    def debug(self, msg: str, *args: object) -> None: ...

    def warning(self, msg: str, *args: object) -> None: ...


class CheckpointTracker:
    """Checkpoint persistence policy and delegation wrapper."""

    def __init__(
        self,
        *,
        writer: object,
        requested_symbols_getter: Callable[[], Sequence[str]],
        logger: _LoggerLike,
        save_every: int = 1,
    ) -> None:
        self._writer = writer
        self._requested_symbols_getter = requested_symbols_getter
        self._logger = logger
        self._save_every = max(1, int(save_every))
        self._save_attempts = 0
        self._lock = asyncio.Lock()
        self._conn_lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    def _connection(self) -> sqlite3.Connection | None:
        if isinstance(self._writer, InMemoryBufferWriter):
            return None
        if self._conn is not None:
            return self._conn
        with self._conn_lock:
            if self._conn is None:
                self._conn = open_state_db()
            return self._conn

    def find_latest(self, provider: str, dataset: str) -> Checkpoint | None:
        return find_latest_checkpoint(provider, dataset, conn=self._connection())

    def save(
        self,
        *,
        run_id: str,
        provider: str,
        dataset: str,
        table_name: str | None,
        completed_symbols: set[str],
        failed_symbols: set[str],
        pending_jobs: Sequence[FetchJob],
        status: Literal["in_progress", "completed"] = "in_progress",
        force: bool = False,
    ) -> bool:
        conn = self._connection()
        if conn is None:
            return False
        if not completed_symbols and not failed_symbols and not pending_jobs:
            return False
        self._save_attempts += 1
        if not force and self._save_every > 1 and self._save_attempts % self._save_every != 0:
            return False

        checkpoint = Checkpoint(
            run_id=run_id,
            provider=provider,
            dataset=dataset,
            table_name=table_name,
            completed=list(completed_symbols),
            failed=list(failed_symbols),
            pending_jobs=list(pending_jobs),
            meta={"requested_symbols": list(self._requested_symbols_getter())},
            status=status,
        )
        try:
            save_checkpoint(checkpoint, conn=conn)
            self._logger.debug("PIPELINE: Checkpoint updated for run %s", run_id)
            return True
        except Exception as exc:
            self._logger.warning("PIPELINE: Failed to update checkpoint: %s", exc)
            return False

    async def save_async(
        self,
        *,
        run_id: str,
        provider: str,
        dataset: str,
        table_name: str | None,
        completed_symbols: set[str],
        failed_symbols: set[str],
        pending_jobs: Sequence[FetchJob],
        status: Literal["in_progress", "completed"] = "in_progress",
        force: bool = False,
    ) -> bool:
        async with self._lock:
            return await asyncio.to_thread(
                self.save,
                run_id=run_id,
                provider=provider,
                dataset=dataset,
                table_name=table_name,
                completed_symbols=completed_symbols,
                failed_symbols=failed_symbols,
                pending_jobs=pending_jobs,
                status=status,
                force=force,
            )

    def clear(self, *, table_name: str | None = None) -> int:
        self._save_attempts = 0
        conn = self._connection()
        if conn is None:
            return 0
        return delete_checkpoints(table_name=table_name, conn=conn)

    def close(self) -> None:
        with self._conn_lock:
            conn = self._conn
            self._conn = None
        if conn is None:
            return
        with suppress(Exception):
            conn.close()


__all__ = ["CheckpointTracker", "PendingJobRegistry"]
