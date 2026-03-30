"""Tests for SQLite-backed persistence functionality."""

from __future__ import annotations

from pathlib import Path
import tempfile
from unittest.mock import patch

from vertex_forager.core.checkpoint import (
    Checkpoint,
    find_latest_checkpoint,
    get_cache_dir,
    get_state_db_path,
    list_pending_dlq_entries,
    list_run_history,
    load_checkpoint,
    register_dlq_entry,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.config import FetchJob, RequestSpec, ResolvedClientConfig, RunResult
from vertex_forager.core.errors import RunError


def test_get_cache_dir() -> None:
    with tempfile.TemporaryDirectory() as tmpdir, patch.dict("os.environ", {"HOME": tmpdir}, clear=True):
        cache_dir = get_cache_dir()
        assert cache_dir == Path(tmpdir) / ".cache" / "vertex-forager"
    with tempfile.TemporaryDirectory() as tmpdir, patch.dict("os.environ", {"XDG_CACHE_HOME": tmpdir}, clear=True):
        cache_dir = get_cache_dir()
        assert cache_dir == Path(tmpdir) / "vertex-forager"


def test_checkpoint_model() -> None:
    checkpoint = Checkpoint(
        run_id="test_run_123",
        provider="test_provider",
        dataset="test_dataset",
        completed=["AAPL", "MSFT"],
        failed=["GOOG"],
    )
    assert checkpoint.run_id == "test_run_123"
    assert checkpoint.provider == "test_provider"
    assert checkpoint.dataset == "test_dataset"
    assert checkpoint.completed == ["AAPL", "MSFT"]
    assert checkpoint.failed == ["GOOG"]
    data = checkpoint.model_dump()
    assert data["run_id"] == "test_run_123"
    assert data["completed"] == ["AAPL", "MSFT"]


def test_save_and_load_checkpoint() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        checkpoint = Checkpoint(
            run_id="test_run_123",
            provider="test_provider",
            dataset="test_dataset",
            completed=["AAPL", "MSFT"],
            failed=["GOOG"],
        )
        save_checkpoint(checkpoint)
        assert get_state_db_path().exists()
        loaded = load_checkpoint("test_run_123")
        assert loaded is not None
        assert loaded.run_id == "test_run_123"
        assert loaded.completed == ["AAPL", "MSFT"]
        assert loaded.failed == ["GOOG"]
        assert loaded.pending_jobs == []
        assert load_checkpoint("non_existent") is None


def test_save_and_load_checkpoint_with_pending_jobs() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        pending_job = FetchJob(
            provider="test_provider",
            dataset="test_dataset",
            symbol="AAPL",
            spec=RequestSpec(url="https://example.com", params={"page": 2}),
        )
        checkpoint = Checkpoint(
            run_id="test_run_456",
            provider="test_provider",
            dataset="test_dataset",
            pending_jobs=[pending_job],
        )
        save_checkpoint(checkpoint)
        loaded = load_checkpoint("test_run_456")
        assert loaded is not None
        assert len(loaded.pending_jobs) == 1
        assert loaded.pending_jobs[0].symbol == "AAPL"
        assert loaded.pending_jobs[0].spec.params["page"] == 2


def test_find_latest_checkpoint_uses_sqlite_ordering() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
        patch("vertex_forager.core.checkpoint.time.time", side_effect=[100.0, 100.0, 200.0, 200.0]),
    ):
        save_checkpoint(Checkpoint(run_id="run_old", provider="stub", dataset="prices"))
        save_checkpoint(Checkpoint(run_id="run_new", provider="stub", dataset="prices"))
        latest = find_latest_checkpoint("stub", "prices")
        assert latest is not None
        assert latest.run_id == "run_new"


def test_save_run_history() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        run_result = RunResult(
            provider="test_provider",
            run_id="test_run_123",
            dataset="test_dataset",
            started_at=1000.0,
            finished_at=1100.0,
            duration_s=100.0,
            coverage_pct=95.5,
            tables={"table1": 100, "table2": 200},
            quality_violations={"table1": 3},
            errors=[
                RunError(
                    provider="test_provider",
                    dataset="test_dataset",
                    symbol="",
                    exc_type="ValueError",
                    message="error1",
                    retryable=False,
                ),
                RunError(
                    provider="test_provider",
                    dataset="test_dataset",
                    symbol="",
                    exc_type="ValueError",
                    message="error2",
                    retryable=False,
                ),
            ],
        )
        save_run_history(run_result, "test_run_123")
        history = list_run_history(limit=10)
        assert len(history) == 1
        entry = history[0]
        assert entry["run_id"] == "test_run_123"
        assert entry["provider"] == "test_provider"
        assert entry["dataset"] == "test_dataset"
        assert entry["duration_s"] == 100.0
        assert entry["error_count"] == 2
        assert entry["total_rows"] == 300
        assert entry["coverage_pct"] == 95.5


def test_dlq_index_registration_roundtrip() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
        patch(
            "vertex_forager.utils.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        path = Path(tmpdir) / "dlq" / "prices" / "batch_1.ipc"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"ipc")
        register_dlq_entry(path=path, table="prices", provider="stub", row_count=12)
        entries = list_pending_dlq_entries("prices")
        assert len(entries) == 1
        assert entries[0]["table"] == "prices"
        assert entries[0]["row_count"] == 12
        assert entries[0]["path"] == str(path.resolve())


def test_run_result_coerces_legacy_string_errors() -> None:
    run_result = RunResult(provider="test_provider", errors=["legacy-error"])
    assert len(run_result.errors) == 1
    assert isinstance(run_result.errors[0], RunError)
    assert run_result.errors[0].provider == ""
    assert run_result.errors[0].dataset == ""
    assert run_result.errors[0].symbol == ""
    assert run_result.errors[0].exc_type == "builtins.str"
    assert run_result.errors[0].message == "legacy-error"
    assert run_result.errors[0].retryable is False


def test_resolved_client_config_retention_defaults() -> None:
    config = ResolvedClientConfig(requests_per_minute=60)
    assert config.checkpoint_retention_days == 7
    assert config.run_history_retention_days == 90


def test_resolved_client_config_retention_override() -> None:
    config = ResolvedClientConfig(
        requests_per_minute=60,
        checkpoint_retention_days=3,
        run_history_retention_days=45,
    )
    assert config.checkpoint_retention_days == 3
    assert config.run_history_retention_days == 45
