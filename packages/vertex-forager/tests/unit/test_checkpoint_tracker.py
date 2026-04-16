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

    def _record_save(*args: object, **kwargs: object) -> None:
        saves.append((args, kwargs))

    with (
        patch("vertex_forager.core.runtime_state.save_checkpoint", side_effect=_record_save),
        patch("vertex_forager.core.runtime_state.delete_checkpoints", return_value=3) as delete_mock,
        patch("vertex_forager.core.runtime_state.open_state_db", return_value=object()),
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
        delete_mock.assert_called_once()
        assert delete_mock.call_args.kwargs["table_name"] == "stub_price"
        assert delete_mock.call_args.kwargs["conn"] is not None

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


def test_checkpoint_tracker_reuses_single_state_connection() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("vertex_forager.core.checkpoint.get_cache_dir", return_value=Path(tmpdir)),
    ):
        import vertex_forager.core.checkpoint as checkpoint_mod

        opened = 0
        original_open = checkpoint_mod.open_state_db

        def _open_once() -> object:
            nonlocal opened
            opened += 1
            return original_open()

        with patch("vertex_forager.core.runtime_state.open_state_db", side_effect=_open_once):
            tracker = CheckpointTracker(
                writer=object(),
                requested_symbols_getter=lambda: ["AAPL"],
                logger=_Logger(),
            )

            assert tracker.save(
                run_id="run-1",
                provider="stub",
                dataset="price",
                table_name="stub_price",
                completed_symbols={"AAPL"},
                failed_symbols=set(),
                pending_jobs=[_job()],
            )
            assert tracker.find_latest("stub", "price") is not None
            assert tracker.clear(table_name="stub_price") == 1
            assert opened == 1

            tracker.close()
