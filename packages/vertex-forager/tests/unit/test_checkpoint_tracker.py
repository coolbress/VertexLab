from __future__ import annotations

from pathlib import Path
import tempfile
from unittest.mock import patch

from vertex_forager.core.config import FetchJob, RequestSpec
from vertex_forager.core.runtime_state import CheckpointTracker


class _Logger:
    def debug(self, msg: str, *args: object) -> None:
        pass

    def warning(self, msg: str, *args: object) -> None:
        pass


def _job() -> FetchJob:
    return FetchJob(
        provider="stub",
        dataset="price",
        symbol="AAPL",
        spec=RequestSpec(url="https://example.test", params={"page": 2}),
    )


def test_checkpoint_tracker_save_and_find_latest_roundtrip() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("vertex_forager.core.checkpoint.get_cache_dir", return_value=Path(tmpdir)),
    ):
        tracker = CheckpointTracker(
            writer=object(),
            requested_symbols_getter=lambda: ["AAPL"],
            logger=_Logger(),
        )

        saved = tracker.save(
            run_id="run-1",
            provider="stub",
            dataset="price",
            table_name="stub_price",
            completed_symbols={"AAPL"},
            failed_symbols=set(),
            pending_jobs=[_job()],
        )

        checkpoint = tracker.find_latest("stub", "price")
        assert saved is True
        assert checkpoint is not None
        assert checkpoint.meta["requested_symbols"] == ["AAPL"]
        assert checkpoint.pending_jobs[0].spec.params["page"] == 2


def test_checkpoint_tracker_frequency_policy_and_clear() -> None:
    saves: list[object] = []

    with (
        patch("vertex_forager.core.runtime_state.save_checkpoint", side_effect=saves.append),
        patch("vertex_forager.core.runtime_state.delete_checkpoints", return_value=3) as delete_mock,
    ):
        tracker = CheckpointTracker(
            writer=object(),
            requested_symbols_getter=lambda: ["AAPL"],
            logger=_Logger(),
            save_every=2,
        )

        assert (
            tracker.save(
                run_id="run-1",
                provider="stub",
                dataset="price",
                table_name="stub_price",
                completed_symbols={"AAPL"},
                failed_symbols=set(),
                pending_jobs=[_job()],
            )
            is False
        )
        assert saves == []

        assert tracker.clear(table_name="stub_price") == 3
        delete_mock.assert_called_once_with(table_name="stub_price")

        assert (
            tracker.save(
                run_id="run-2",
                provider="stub",
                dataset="price",
                table_name="stub_price",
                completed_symbols={"AAPL"},
                failed_symbols=set(),
                pending_jobs=[_job()],
            )
            is False
        )
        assert (
            tracker.save(
                run_id="run-2",
                provider="stub",
                dataset="price",
                table_name="stub_price",
                completed_symbols={"AAPL"},
                failed_symbols=set(),
                pending_jobs=[_job()],
            )
            is True
        )
        assert len(saves) == 1
