from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any

import click
from click.testing import CliRunner
import pytest

from vertex_forager import cli as cli_mod
from vertex_forager.core.config import RunResult
from vertex_forager.state.dlq import DLQEntry, ReplayResult
from vertex_forager.state.runs import RunRecord


def _run_record(*, run_id: str, table: str) -> RunRecord:
    return RunRecord(
        run_id=run_id,
        provider="yfinance",
        dataset="price",
        table_name=table,
        started_at=1_700_000_000.0,
        finished_at=1_700_000_010.0,
        duration_s=10.0,
        tables={table: 12},
        error_count=1,
        errors=[{"message": "boom", "retryable": False}],
        quality_violations={table: 2},
        coverage_pct=95.0,
        created_at=1_700_000_010.0,
    )


def _dlq_entry(*, table: str, status: str = "pending") -> DLQEntry:
    return DLQEntry(
        provider="yfinance",
        table=table,
        row_count=3,
        retry_count=1,
        status=status,
        created_at=datetime.fromtimestamp(1_700_000_000.0, tz=timezone.utc),
        path=Path(f"{table}.ipc"),
    )


def test_status_runs_and_prints_state_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "root"
    cache = root / "cache"
    cache.mkdir(parents=True)
    (root / "artifact.txt").write_text("x" * 10, encoding="utf-8")
    state_db = cache / "state.db"
    with sqlite3.connect(state_db) as conn:
        conn.execute("CREATE TABLE checkpoints (table_name TEXT, status TEXT, updated_at REAL)")
        conn.execute("CREATE TABLE dlq_index (table_name TEXT, status TEXT)")
        conn.execute("CREATE TABLE run_history (table_name TEXT, created_at REAL)")
        conn.execute("INSERT INTO checkpoints VALUES ('sharadar_price', 'in_progress', 1.0)")
        conn.execute("INSERT INTO dlq_index VALUES ('yfinance_price', 'pending')")
        conn.execute("INSERT INTO run_history VALUES ('yfinance_price', 1700000000.0)")
        conn.commit()

    monkeypatch.setattr(cli_mod, "get_app_root", lambda: root, raising=True)
    monkeypatch.setattr(cli_mod, "get_cache_dir", lambda: cache, raising=True)
    monkeypatch.setattr(cli_mod, "get_state_db_path", lambda: state_db, raising=True)

    result = CliRunner().invoke(cli_mod.main, ["status"])
    assert result.exit_code == 0
    assert "Checkpoint entries per table" in result.output
    assert "sharadar_price: 1" in result.output
    assert "Pending DLQ batches per table" in result.output
    assert "yfinance_price: 1" in result.output
    assert "Last run timestamp per table" in result.output


def test_clear_confirms_and_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, bool] = {}

    monkeypatch.setattr(click, "confirm", lambda _: True, raising=True)
    monkeypatch.setattr(cli_mod, "clear_app_cache", lambda: called.setdefault("ok", True), raising=True)

    result = CliRunner().invoke(cli_mod.main, ["clear"])
    assert result.exit_code == 0
    assert called["ok"] is True
    assert "Cache cleared" in result.output


def test_constants_json_global() -> None:
    result = CliRunner().invoke(cli_mod.main, ["constants", "--section", "global", "--format", "json"])
    assert result.exit_code == 0
    assert "DEFAULT_RATE_LIMIT" in result.output


@pytest.mark.parametrize(
    ("argv", "method_name"),
    [
        (["collect", "sharadar", "price", "--symbol", "AAPL"], "get_price_data"),
        (["collect", "sharadar", "fundamentals", "--symbol", "AAPL"], "get_fundamental_data"),
        (["collect", "sharadar", "tickers"], "get_ticker_info"),
        (["collect", "sharadar", "sp500"], "get_sp500_history"),
        (["collect", "yfinance", "price", "--symbol", "AAPL"], "get_price_data"),
        (
            ["collect", "yfinance", "financials", "--symbol", "AAPL", "--kind", "income_stmt"],
            "get_financials",
        ),
        (["collect", "yfinance", "info", "--symbol", "AAPL"], "get_info"),
        (["collect", "yfinance", "dividends", "--symbol", "AAPL"], "get_actions"),
    ],
)
def test_collect_subcommands_invoke_expected_methods(
    argv: list[str],
    method_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class _FakeClient:
        def __getattr__(self, name: str) -> Any:
            def _run(**kwargs: Any) -> RunResult:
                calls.append((name, kwargs))
                return RunResult(provider="stub", dataset="demo", tables={"demo_table": 2})

            return _run

    monkeypatch.setattr(cli_mod, "create_client", lambda **_: _FakeClient(), raising=True)
    result = CliRunner().invoke(cli_mod.main, argv)
    assert result.exit_code == 0
    assert calls[0][0] == method_name
    assert "Run completed" in result.output


def test_collect_financials_requires_kind() -> None:
    result = CliRunner().invoke(cli_mod.main, ["collect", "yfinance", "financials", "--symbol", "AAPL"])
    assert result.exit_code != 0
    assert "--kind" in result.output


def test_collect_resume_flag_absent() -> None:
    result = CliRunner().invoke(
        cli_mod.main,
        ["collect", "sharadar", "price", "--symbol", "AAPL", "--resume"],
    )
    assert result.exit_code != 0
    assert "No such option" in result.output


def test_runs_list_outputs_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Runs:
        def list(self, *, table: str | None = None, limit: int = 20) -> list[RunRecord]:
            assert table == "yfinance_price"
            assert limit == 5
            return [_run_record(run_id="run-1", table="yfinance_price")]

    class _StateManager:
        def __init__(self) -> None:
            self.runs = _Runs()

    monkeypatch.setattr(cli_mod, "StateManager", _StateManager, raising=True)
    result = CliRunner().invoke(cli_mod.main, ["runs", "list", "--table", "yfinance_price", "--limit", "5"])
    assert result.exit_code == 0
    assert "run_id=run-1" in result.output
    assert "table=yfinance_price" in result.output


def test_runs_clear_requires_filter() -> None:
    result = CliRunner().invoke(cli_mod.main, ["runs", "clear"])
    assert result.exit_code != 0
    assert "Provide --table, --before, or both." in result.output


def test_runs_clear_delegates_table_and_before(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, Any] = {}

    class _Runs:
        def clear(self, *, table: str | None = None, before_days: int | None = None) -> int:
            called["table"] = table
            called["before_days"] = before_days
            return 4

    class _StateManager:
        def __init__(self) -> None:
            self.runs = _Runs()

    monkeypatch.setattr(cli_mod, "StateManager", _StateManager, raising=True)
    result = CliRunner().invoke(cli_mod.main, ["runs", "clear", "--table", "sharadar_price", "--before", "30d"])
    assert result.exit_code == 0
    assert called == {"table": "sharadar_price", "before_days": 30}
    assert "Deleted 4 run history rows." in result.output


def test_dlq_list_applies_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DLQ:
        def list(self, *, table: str | None = None, status: str | None = "pending") -> list[DLQEntry]:
            assert table == "yfinance_price"
            assert status is None
            return [_dlq_entry(table="yfinance_price", status="recovered")]

    class _StateManager:
        def __init__(self) -> None:
            self.dlq = _DLQ()

    monkeypatch.setattr(cli_mod, "StateManager", _StateManager, raising=True)
    result = CliRunner().invoke(
        cli_mod.main,
        ["dlq", "list", "--table", "yfinance_price", "--status", "all"],
    )
    assert result.exit_code == 0
    assert "status=recovered" in result.output


def test_dlq_replay_uses_stored_output_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, Any] = {}

    class _DLQ:
        def replay(self, *, table: str, output: str, dry_run: bool = False) -> ReplayResult:
            called["table"] = table
            called["output"] = output
            called["dry_run"] = dry_run
            return ReplayResult(replayed=1, failed=0, skipped=0, errors=[])

    class _StateManager:
        def __init__(self) -> None:
            self.dlq = _DLQ()

    monkeypatch.setattr(cli_mod, "StateManager", _StateManager, raising=True)
    monkeypatch.setattr(
        cli_mod,
        "list_dlq_entries",
        lambda **_: [{"output_uri": "duckdb:///stored.duckdb"}],
        raising=True,
    )
    result = CliRunner().invoke(cli_mod.main, ["dlq", "replay", "--table", "yfinance_price"])
    assert result.exit_code == 0
    assert called["output"] == "duckdb:///stored.duckdb"
    assert "replayed=1" in result.output


def test_dlq_clear_requires_table_or_all() -> None:
    result = CliRunner().invoke(cli_mod.main, ["dlq", "clear"])
    assert result.exit_code != 0
    assert "Provide --table or --all." in result.output


def test_dlq_clear_all_confirms(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DLQ:
        def clear(self, *, table: str | None = None) -> int:
            assert table is None
            return 5

    class _StateManager:
        def __init__(self) -> None:
            self.dlq = _DLQ()

    monkeypatch.setattr(cli_mod, "StateManager", _StateManager, raising=True)
    monkeypatch.setattr(click, "confirm", lambda _: True, raising=True)
    result = CliRunner().invoke(cli_mod.main, ["dlq", "clear", "--all"])
    assert result.exit_code == 0
    assert "Deleted 5 DLQ rows." in result.output


def test_checkpoints_resume_constructs_provider_client(monkeypatch: pytest.MonkeyPatch) -> None:
    create_calls: list[dict[str, Any]] = []
    resume_calls: list[dict[str, Any]] = []

    class _Checkpoints:
        def resume(self, *, table: str, client: Any, output: str) -> RunResult:
            resume_calls.append({"table": table, "client": client, "output": output})
            return RunResult(provider="sharadar", dataset="price", tables={"sharadar_price": 3})

    class _StateManager:
        def __init__(self) -> None:
            self.checkpoints = _Checkpoints()

    class _Client:
        pass

    monkeypatch.setattr(cli_mod, "StateManager", _StateManager, raising=True)
    monkeypatch.setattr(
        cli_mod,
        "create_client",
        lambda **kwargs: create_calls.append(kwargs) or _Client(),
        raising=True,
    )
    result = CliRunner().invoke(
        cli_mod.main,
        ["checkpoints", "resume", "--table", "sharadar_price", "--output", "duckdb:///data.db"],
    )
    assert result.exit_code == 0
    assert create_calls[0]["provider"] == "sharadar"
    assert create_calls[0]["rate_limit"] == cli_mod.DEFAULT_RATE_LIMIT
    assert resume_calls[0]["table"] == "sharadar_price"
    assert "Run completed" in result.output


def test_checkpoints_clear_requires_table_or_all() -> None:
    result = CliRunner().invoke(cli_mod.main, ["checkpoints", "clear"])
    assert result.exit_code != 0
    assert "Provide --table or --all." in result.output


def test_recover_command_removed() -> None:
    result = CliRunner().invoke(cli_mod.main, ["recover"])
    assert result.exit_code != 0
    assert "No such command 'recover'" in result.output
