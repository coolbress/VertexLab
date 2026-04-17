from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from vertex_forager.core.progress import ProgressEmitter


class _Proc:
    def __init__(self) -> None:
        self._cpu_calls = 0

    def cpu_percent(self, interval: object = None) -> float:
        self._cpu_calls += 1
        return 7.5 if self._cpu_calls > 1 else 0.0

    def memory_info(self) -> object:
        return type("_Mem", (), {"rss": 64 * 1024 * 1024})()


def test_progress_emitter_initializes_psutil_lazily() -> None:
    with patch("vertex_forager.core.progress.psutil.Process") as process_cls:
        emitter = ProgressEmitter()
        emitter.reset(jobs_total=1)
        assert emitter._process is None
        process_cls.assert_not_called()

        emitter.build_snapshot(
            pending_jobs=0,
            errors=0,
            retries=0,
            throttle_events=0,
            finished=False,
        )

        process_cls.assert_called_once()
        emitter.reset(jobs_total=2)
        assert emitter._process is None


def test_build_snapshot_uses_counter_state_and_pending_len() -> None:
    proc = _Proc()
    with patch("vertex_forager.core.progress.psutil.Process", return_value=proc):
        emitter = ProgressEmitter()
        emitter.reset(jobs_total=10, jobs_done_initial=2)
        emitter.inc_counter("rows_written_total", 15)
        emitter.inc_counter("dlq_spooled_files_total", 3)
        emitter.worker_started()
        emitter.record_terminal(2)
        snapshot = emitter.build_snapshot(
            pending_jobs=4,
            errors=1,
            retries=2,
            throttle_events=5,
            finished=False,
        )

    assert snapshot.jobs_done == 4
    assert snapshot.jobs_total == 10
    assert snapshot.pending_jobs == 4
    assert snapshot.rows_written == 15
    assert snapshot.dlq_spooled == 3
    assert snapshot.errors == 1
    assert snapshot.retries == 2
    assert snapshot.throttle_events == 5
    assert snapshot.active_workers == 1
    assert snapshot.memory_mb == 64.0
    assert snapshot.cpu_pct == 7.5


@pytest.mark.asyncio
async def test_emit_invokes_callback_and_progress_bar() -> None:
    proc = _Proc()
    with patch("vertex_forager.core.progress.psutil.Process", return_value=proc):
        emitter = ProgressEmitter()
        emitter.reset(jobs_total=5)
        emitter.record_terminal(3)
        snapshot = emitter.build_snapshot(
            pending_jobs=1,
            errors=0,
            retries=0,
            throttle_events=0,
            finished=True,
            result_duration_s=2.0,
        )

    seen: list[object] = []

    async def on_progress(progress_snapshot: object) -> None:
        seen.append(progress_snapshot)

    progress_bar = MagicMock()
    logger = MagicMock()

    await emitter.emit(
        snapshot=snapshot,
        on_progress=on_progress,
        progress_bar=progress_bar,
        show_summary=True,
        provider="stub",
        dataset="price",
        logger=logger,
    )

    assert seen == [snapshot]
    progress_bar.update.assert_called_once_with(3)
    progress_bar.set_postfix.assert_called_once()
    progress_bar.close.assert_called_once()
    logger.error.assert_not_called()
