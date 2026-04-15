"""Tests for SQLite-backed persistence functionality."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
from unittest.mock import patch

import pytest

from vertex_forager.core.checkpoint import (
    Checkpoint,
    cleanup_state_retention,
    delete_checkpoints,
    delete_dlq_entries,
    delete_dlq_entry,
    delete_run_history,
    find_latest_checkpoint,
    get_cache_dir,
    get_state_db_path,
    list_dlq_entries,
    list_pending_dlq_entries,
    list_run_history,
    load_checkpoint,
    mark_dlq_retry_result,
    register_dlq_entry,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.config import FetchJob, RequestSpec, ResolvedClientConfig, RunResult
from vertex_forager.exceptions import RunError


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
        table_name="test_provider_test_dataset",
        completed=["AAPL", "MSFT"],
        failed=["GOOG"],
    )
    assert checkpoint.run_id == "test_run_123"
    assert checkpoint.provider == "test_provider"
    assert checkpoint.dataset == "test_dataset"
    assert checkpoint.table_name == "test_provider_test_dataset"
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
            table_name="test_provider_test_dataset",
            completed=["AAPL", "MSFT"],
            failed=["GOOG"],
        )
        save_checkpoint(checkpoint)
        assert get_state_db_path().exists()
        loaded = load_checkpoint("test_run_123")
        assert loaded is not None
        assert loaded.run_id == "test_run_123"
        assert loaded.table_name == "test_provider_test_dataset"
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
            table_name="test_provider_test_dataset",
            pending_jobs=[pending_job],
        )
        save_checkpoint(checkpoint)
        loaded = load_checkpoint("test_run_456")
        assert loaded is not None
        assert len(loaded.pending_jobs) == 1
        assert loaded.pending_jobs[0].symbol == "AAPL"
        assert loaded.pending_jobs[0].spec.params["page"] == 2


def test_delete_run_history_and_checkpoints_with_filters() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("vertex_forager.core.checkpoint.get_cache_dir", return_value=Path(tmpdir)),
        patch("vertex_forager.core.checkpoint.time.time", side_effect=[100.0, 100.0, 200.0, 200.0]),
    ):
        save_checkpoint(Checkpoint(run_id="keep", provider="stub", dataset="prices", table_name="keep_table"))
        save_checkpoint(Checkpoint(run_id="drop", provider="stub", dataset="prices", table_name="drop_table"))

        save_run_history(
            RunResult(
                run_id="run-keep",
                provider="stub",
                dataset="prices",
                table_name="keep_table",
                total_rows=1,
                errors=[],
                quality_violations={},
            ),
            "run-keep",
            table_name="keep_table",
        )
        save_run_history(
            RunResult(
                run_id="run-drop",
                provider="stub",
                dataset="prices",
                table_name="drop_table",
                total_rows=2,
                errors=[],
                quality_violations={},
            ),
            "run-drop",
            table_name="drop_table",
        )

        assert delete_checkpoints(table_name="drop_table") == 1
        assert find_latest_checkpoint(table_name="drop_table") is None
        assert find_latest_checkpoint(table_name="keep_table") is not None

        assert delete_run_history(table_name="drop_table") == 1
        history = list_run_history(limit=10)
        assert [entry["table_name"] for entry in history] == ["keep_table"]


def test_find_latest_checkpoint_uses_sqlite_ordering() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
        patch("vertex_forager.core.checkpoint.time.time", side_effect=[100.0, 100.0, 200.0, 200.0]),
    ):
        save_checkpoint(Checkpoint(run_id="run_old", provider="stub", dataset="prices", table_name="stub_prices"))
        save_checkpoint(Checkpoint(run_id="run_new", provider="stub", dataset="prices", table_name="stub_prices"))
        latest = find_latest_checkpoint(table_name="stub_prices")
        assert latest is not None
        assert latest.run_id == "run_new"


def test_mark_dlq_retry_result_and_delete_dlq_entries() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("vertex_forager.core.checkpoint.get_cache_dir", return_value=Path(tmpdir)),
    ):
        dlq_root = Path(tmpdir) / "dlq"
        dlq_root.mkdir(parents=True, exist_ok=True)
        payload_path = dlq_root / "failed" / "pkt.ipc"
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        payload_path.write_bytes(b"payload")

        register_dlq_entry(
            path=payload_path,
            table="yfinance_price",
            provider="yfinance",
            row_count=3,
            output_uri="duckdb:///forager.duckdb",
        )

        mark_dlq_retry_result(path=payload_path.resolve(), success=False, error="still failing")
        pending = list_dlq_entries(provider="yfinance", status="pending")
        assert len(pending) == 1
        assert pending[0]["retry_count"] == 1
        assert pending[0]["last_error"] == "still failing"

        mark_dlq_retry_result(path=payload_path.resolve(), success=True)
        recovered = list_dlq_entries(provider="yfinance", status="recovered")
        assert len(recovered) == 1
        assert recovered[0]["retry_count"] == 2

        deleted = delete_dlq_entries(provider="yfinance", status="recovered")
        assert deleted == {"rows": 1, "files": 1}
        assert not payload_path.exists()
        assert list_dlq_entries(provider="yfinance", status=None) == []


def test_cleanup_state_retention_removes_old_state() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("vertex_forager.core.checkpoint.get_cache_dir", return_value=Path(tmpdir)),
    ):
        with patch("vertex_forager.core.checkpoint.time.time", return_value=100.0):
            save_checkpoint(
                Checkpoint(
                    run_id="old-run",
                    provider="stub",
                    dataset="prices",
                    table_name="stub_prices",
                    status="completed",
                )
            )
        save_run_history(
            RunResult(
                run_id="old-run",
                provider="stub",
                dataset="prices",
                table_name="stub_prices",
                total_rows=1,
                started_at=100.0,
                finished_at=100.0,
                errors=[],
                quality_violations={},
            ),
            "old-run",
            table_name="stub_prices",
        )

        dlq_root = Path(tmpdir) / "dlq"
        dlq_root.mkdir(parents=True, exist_ok=True)
        payload_path = dlq_root / "stale.ipc"
        payload_path.write_bytes(b"payload")
        with patch("vertex_forager.core.checkpoint.time.time", return_value=100.0):
            register_dlq_entry(
                path=payload_path,
                table="stub_prices",
                provider="stub",
                row_count=1,
                output_uri=None,
            )

        with patch("vertex_forager.core.checkpoint.time.time", return_value=5000.0):
            result = cleanup_state_retention(
                checkpoint_retention_days=0,
                run_history_retention_days=0,
                dlq_retention_s=0,
            )

        assert result == {"checkpoints": 1, "runs": 1, "dlq_rows": 1, "dlq_files": 1}
        assert find_latest_checkpoint(table_name="stub_prices") is None
        assert list_run_history(limit=10) == []
        assert list_dlq_entries(status=None) == []


def test_initialize_schema_marks_legacy_in_progress_checkpoints_completed() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        db_path = get_state_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE checkpoints (
                    run_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    completed_json TEXT NOT NULL,
                    failed_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO checkpoints (
                    run_id,
                    provider,
                    dataset,
                    completed_json,
                    failed_json,
                    status,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("legacy-run", "stub", "prices", "[]", "[]", "in_progress", 100.0),
            )
            conn.commit()

        latest = find_latest_checkpoint(table_name="stub_prices")
        assert latest is None

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT table_name, status FROM checkpoints WHERE run_id = ?", ("legacy-run",)
            ).fetchone()
            assert row is not None
            assert row[0] is None
            assert row[1] == "completed"


def test_migrate_run_history_preserves_empty_table_runs() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        db_path = get_state_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE run_history (
                    run_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    dataset TEXT,
                    started_at REAL,
                    finished_at REAL,
                    duration_s REAL,
                    tables_json TEXT NOT NULL,
                    error_count INTEGER NOT NULL,
                    errors_json TEXT NOT NULL,
                    quality_violations_json TEXT NOT NULL,
                    coverage_pct REAL,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO run_history (
                    run_id,
                    provider,
                    dataset,
                    started_at,
                    finished_at,
                    duration_s,
                    tables_json,
                    error_count,
                    errors_json,
                    quality_violations_json,
                    coverage_pct,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("legacy-empty", "stub", "prices", 100.0, 110.0, 10.0, "{}", 1, "[]", "{}", None, 120.0),
            )
            conn.commit()

        history = list_run_history(limit=10)
        assert len(history) == 1
        assert history[0]["run_id"] == "legacy-empty"
        assert history[0]["table_name"] is None
        assert history[0]["tables"] == {}
        assert history[0]["total_rows"] == 0


def test_migrate_run_history_rolls_back_on_failure() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        db_path = get_state_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE run_history (
                    run_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    dataset TEXT,
                    started_at REAL,
                    finished_at REAL,
                    duration_s REAL,
                    tables_json TEXT NOT NULL,
                    error_count INTEGER NOT NULL,
                    errors_json TEXT NOT NULL,
                    quality_violations_json TEXT NOT NULL,
                    coverage_pct REAL,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO run_history (
                    run_id,
                    provider,
                    dataset,
                    started_at,
                    finished_at,
                    duration_s,
                    tables_json,
                    error_count,
                    errors_json,
                    quality_violations_json,
                    coverage_pct,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("legacy-bad", "stub", "prices", 100.0, 110.0, 10.0, "{}", "bad", "[]", "{}", None, 120.0),
            )
            conn.commit()

        with pytest.raises(ValueError, match="invalid literal for int"):
            list_run_history(limit=10)

        with sqlite3.connect(db_path) as conn:
            table_names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            assert "run_history" in table_names
            assert "run_history_legacy" not in table_names
            row = conn.execute("SELECT run_id, error_count FROM run_history").fetchone()
            assert row == ("legacy-bad", "bad")


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
        assert len(history) == 2
        table1_history = list_run_history(limit=10, table_name="table1")
        table2_history = list_run_history(limit=10, table_name="table2")
        assert len(table1_history) == 1
        assert len(table2_history) == 1
        entry = table1_history[0]
        assert entry["run_id"] == "test_run_123"
        assert entry["provider"] == "test_provider"
        assert entry["dataset"] == "test_dataset"
        assert entry["table_name"] == "table1"
        assert entry["duration_s"] == 100.0
        assert entry["error_count"] == 2
        assert entry["total_rows"] == 100
        assert entry["coverage_pct"] == 95.5
        assert table2_history[0]["total_rows"] == 200


def test_save_run_history_preserves_empty_table_runs() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        run_result = RunResult(
            provider="test_provider",
            run_id="empty_run",
            dataset="test_dataset",
            started_at=1000.0,
            finished_at=1100.0,
            duration_s=100.0,
            tables={},
            errors=[
                RunError(
                    provider="test_provider",
                    dataset="test_dataset",
                    symbol="",
                    exc_type="ValueError",
                    message="failed before write",
                    retryable=False,
                )
            ],
        )
        save_run_history(run_result, "empty_run")
        history = list_run_history(limit=10)
        assert len(history) == 1
        assert history[0]["run_id"] == "empty_run"
        assert history[0]["table_name"] is None
        assert history[0]["tables"] == {}
        assert history[0]["total_rows"] == 0


def test_dlq_index_registration_roundtrip() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        path = Path(tmpdir) / "dlq" / "prices" / "batch_1.ipc"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"ipc")
        register_dlq_entry(
            path=path, table="prices", provider="stub", row_count=12, output_uri="duckdb:///tmp/test.duckdb"
        )
        entries = list_pending_dlq_entries("prices")
        assert len(entries) == 1
        assert entries[0]["table"] == "prices"
        assert entries[0]["row_count"] == 12
        assert entries[0]["path"] == str(path.resolve())


def test_delete_dlq_entry_validates_root_and_deletes_db_row() -> None:
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch(
            "vertex_forager.core.checkpoint.get_cache_dir",
            return_value=Path(tmpdir),
        ),
    ):
        path = Path(tmpdir) / "dlq" / "prices" / "batch_1.ipc"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"ipc")
        register_dlq_entry(
            path=path, table="prices", provider="stub", row_count=12, output_uri="duckdb:///tmp/test.duckdb"
        )

        assert delete_dlq_entry(path) is True
        assert not path.exists()
        assert list_pending_dlq_entries("prices") == []

        outside_path = Path(tmpdir) / "outside.ipc"
        outside_path.write_bytes(b"ipc")
        with pytest.raises(ValueError, match="outside DLQ root"):
            delete_dlq_entry(outside_path)


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
    assert config.storage.checkpoint_retention_days == 7
    assert config.storage.run_history_retention_days == 90


def test_resolved_client_config_retention_override() -> None:
    config = ResolvedClientConfig(
        requests_per_minute=60,
        storage={"checkpoint_retention_days": 3, "run_history_retention_days": 45},
    )
    assert config.storage.checkpoint_retention_days == 3
    assert config.storage.run_history_retention_days == 45
