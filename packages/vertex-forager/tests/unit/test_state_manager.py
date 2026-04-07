from __future__ import annotations

from datetime import date
from pathlib import Path
import sqlite3
import time

import duckdb
import polars as pl
import pytest

from vertex_forager import DLQEntry, ReplayResult, RunRecord, StateManager, create_client
from vertex_forager.core.checkpoint import (
    Checkpoint,
    get_cache_dir,
    get_state_db_path,
    list_dlq_entries,
    register_dlq_entry,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.config import ResolvedClientConfig, RunResult, StorageConfig
from vertex_forager.core.controller import FlowController
from vertex_forager.core.errors import RunError
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.exceptions import CheckpointNotFoundError
from vertex_forager.writers.memory import InMemoryBufferWriter


def test_state_manager_exports_and_runs_filters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    state = StateManager()
    assert isinstance(state.dlq.list(), list)
    assert state.db_path == get_state_db_path()
    assert DLQEntry.__name__ == "DLQEntry"
    assert ReplayResult.__name__ == "ReplayResult"
    assert RunRecord.__name__ == "RunRecord"

    save_run_history(
        RunResult(provider="sharadar", run_id="run-1", dataset="price", tables={"sharadar_price": 2}),
        "run-1",
    )
    save_run_history(
        RunResult(provider="yfinance", run_id="run-2", dataset="price", tables={"yfinance_price": 1}),
        "run-2",
    )

    with sqlite3.connect(get_state_db_path()) as conn:
        conn.execute("UPDATE run_history SET created_at = ? WHERE run_id = ?", (time.time() - (40 * 86_400), "run-1"))
        conn.commit()

    yfinance_runs = state.runs.list(table="yfinance_price", limit=5)
    assert [record.run_id for record in yfinance_runs] == ["run-2"]
    assert yfinance_runs[0].table_name == "yfinance_price"
    cleared = state.runs.clear(table="sharadar_price", before_days=30)
    assert cleared == 1
    assert [record.run_id for record in state.runs.list(limit=10)] == ["run-2"]
    with pytest.raises(ValueError, match="non-negative"):
        state.runs.clear(before_days=-1)


def test_state_manager_runs_preserve_boolean_error_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    state = StateManager()

    save_run_history(
        RunResult(
            provider="yfinance",
            run_id="run-bool",
            dataset="price",
            tables={"yfinance_price": 1},
            errors=[
                RunError(
                    provider="yfinance",
                    dataset="price",
                    symbol="AAPL",
                    exc_type="ValueError",
                    message="boom",
                    retryable=False,
                )
            ],
        ),
        "run-bool",
    )

    records = state.runs.list(table="yfinance_price", limit=5)
    assert len(records) == 1
    assert records[0].errors[0]["retryable"] is False


def test_state_manager_dlq_list_clear_and_replay(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    state = StateManager()
    dlq_dir = get_cache_dir() / "dlq" / "yfinance_price"
    dlq_dir.mkdir(parents=True, exist_ok=True)
    good_path = dlq_dir / "batch_good.ipc"
    missing_path = dlq_dir / "batch_missing.ipc"
    bad_path = dlq_dir / "batch_bad.ipc"
    frame = pl.DataFrame(
        {
            "provider": ["yfinance"],
            "ticker": ["AAPL"],
            "date": [date(2024, 1, 1)],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
        }
    )
    frame.write_ipc(good_path)
    bad_path.write_text("not-an-ipc", encoding="utf-8")
    out_db = tmp_path / "replay.duckdb"

    register_dlq_entry(path=good_path, table="yfinance_price", provider="yfinance", row_count=1, output_uri=None)
    register_dlq_entry(
        path=missing_path,
        table="yfinance_price",
        provider="yfinance",
        row_count=1,
        output_uri=None,
    )
    register_dlq_entry(path=bad_path, table="yfinance_price", provider="yfinance", row_count=1, output_uri=None)

    pending = state.dlq.list(table="yfinance_price", status="pending")
    assert len(pending) == 3

    dry_run = state.dlq.replay(table="yfinance_price", output=f"duckdb://{out_db}", dry_run=True)
    assert dry_run == ReplayResult(replayed=2, failed=0, skipped=1, errors=[])

    result = state.dlq.replay(table="yfinance_price", output=f"duckdb://{out_db}")
    assert result.replayed == 1
    assert result.failed == 1
    assert result.skipped == 1

    with duckdb.connect(str(out_db)) as conn:
        count = conn.execute("SELECT count(*) FROM yfinance_price").fetchone()
        assert count is not None
        assert count[0] == 1

    recovered = state.dlq.list(table="yfinance_price", status="recovered")
    assert len(recovered) == 1

    cleared = state.dlq.clear(table="yfinance_price")
    assert cleared == 3
    assert list_dlq_entries(table="yfinance_price", status=None) == []


def test_state_manager_dlq_replay_does_not_fail_when_success_mark_update_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    state = StateManager()
    dlq_dir = get_cache_dir() / "dlq" / "yfinance_price"
    dlq_dir.mkdir(parents=True, exist_ok=True)
    good_path = dlq_dir / "batch_good.ipc"
    pl.DataFrame(
        {
            "provider": ["yfinance"],
            "ticker": ["AAPL"],
            "date": [date(2024, 1, 1)],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
        }
    ).write_ipc(good_path)
    out_db = tmp_path / "replay.duckdb"
    register_dlq_entry(path=good_path, table="yfinance_price", provider="yfinance", row_count=1, output_uri=None)

    def _mark_dlq_retry_result(*, path: Path, success: bool, error: str | None = None) -> None:
        if success:
            raise RuntimeError("state update failed")

    monkeypatch.setattr("vertex_forager.state.dlq.mark_dlq_retry_result", _mark_dlq_retry_result, raising=True)

    result = state.dlq.replay(table="yfinance_price", output=f"duckdb://{out_db}")
    assert result.replayed == 1
    assert result.failed == 0
    assert result.skipped == 0
    assert result.errors == []


def test_state_manager_checkpoints_resume_and_clear(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))
    state = StateManager()
    client = create_client(provider="yfinance")
    save_checkpoint(
        Checkpoint(
            run_id="cp-1",
            provider="yfinance",
            dataset="price",
            table_name="yfinance_price",
            meta={"requested_symbols": ["AAPL"]},
        )
    )

    captured: dict[str, object] = {}

    async def _resume_dataset_async(
        *, dataset: str, checkpoint: Checkpoint, output: str, **kwargs: object
    ) -> RunResult:
        captured["dataset"] = dataset
        captured["run_id"] = checkpoint.run_id
        captured["output"] = output
        captured["kwargs"] = kwargs
        return RunResult(provider="yfinance", dataset=dataset)

    monkeypatch.setattr(client, "_resume_dataset_async", _resume_dataset_async, raising=True)

    result = state.checkpoints.resume(
        table="yfinance_price",
        client=client,
        output=f"duckdb://{tmp_path / 'resume.duckdb'}",
    )
    assert result.dataset == "price"
    assert captured["run_id"] == "cp-1"

    with pytest.raises(ValueError, match="persisted output"):
        state.checkpoints.resume(table="yfinance_price", client=client, output="memory://")

    state.checkpoints.clear(table="yfinance_price")
    with pytest.raises(CheckpointNotFoundError):
        state.checkpoints.resume(table="yfinance_price", client=client, output=f"duckdb://{tmp_path / 'resume.duckdb'}")


def test_flow_controller_runtime_properties() -> None:
    controller = FlowController(requests_per_minute=90, concurrency_limit=2)
    assert controller.rpm_ceiling == 90
    assert controller.effective_rpm == 90


def test_inmemory_writer_skips_checkpoint_persistence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "vf-root"))

    class _Router:
        provider = "stub"

    class _Http:
        async def fetch(self, spec: object) -> bytes:
            return b""

    engine = VertexForager(
        router=_Router(),
        http=_Http(),
        writer=InMemoryBufferWriter(),
        mapper=None,
        config=ResolvedClientConfig(requests_per_minute=60, storage=StorageConfig()),
        controller=FlowController(requests_per_minute=60, concurrency_limit=1),
    )
    engine._requested_symbols = ["AAPL"]
    engine._update_checkpoint("run-1", "stub", "price", "stub_price", {"AAPL"}, set(), [], "completed")
    assert not get_state_db_path().exists()
