from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
from click.testing import CliRunner
import polars as pl
import pytest

from vertex_forager import cli as cli_mod
from vertex_forager.utils import get_cache_dir


def test_status_runs_and_prints_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["status"])
    assert result.exit_code == 0
    assert "Data Root" in result.output
    assert "Cache Dir" in result.output


def test_clear_confirms_and_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, Any] = {}

    def _confirm(_: str) -> bool:
        return True

    def _clear() -> None:
        called["ok"] = True

    monkeypatch.setattr(click, "confirm", _confirm, raising=True)
    monkeypatch.setattr(cli_mod, "clear_app_cache", _clear, raising=True)
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["clear"])
    assert result.exit_code == 0
    assert called.get("ok") is True
    assert "Cache cleared" in result.output


def test_constants_json_global() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["constants", "--section", "global", "--format", "json"])
    assert result.exit_code == 0
    assert "HTTP_TIMEOUT_S" in result.output


def test_clear_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    def _confirm(_: str) -> bool:
        return False

    monkeypatch.setattr(click, "confirm", _confirm, raising=True)
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["clear"])
    assert result.exit_code == 0
    assert result.output.strip() == ""


def test_clear_runs_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(click, "confirm", lambda _: True, raising=True)
    monkeypatch.setattr(cli_mod, "delete_all_run_history", lambda: 4, raising=True)
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["clear", "--runs"])
    assert result.exit_code == 0
    assert "runs=4" in result.output


def test_runs_list_outputs_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli_mod,
        "list_run_history",
        lambda limit=20: [
            {
                "run_id": "run-1",
                "provider": "yfinance",
                "dataset": "price",
                "total_rows": 123,
                "error_count": 1,
                "finished_at": 1_700_000_000.0,
            }
        ],
        raising=True,
    )
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["runs", "list"])
    assert result.exit_code == 0
    assert "run_id=run-1" in result.output
    assert "rows=123" in result.output


def test_dlq_list_outputs_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli_mod,
        "list_pending_dlq_entries",
        lambda table=None: [
            {
                "table": "prices",
                "row_count": 7,
                "created_at": 1_700_000_000.0,
                "path": "cache/dlq/prices/batch_1.ipc",
                "retry_count": 2,
            }
        ],
        raising=True,
    )
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["dlq", "list"])
    assert result.exit_code == 0
    assert "table=prices" in result.output
    assert "path=batch_1.ipc" in result.output


def test_collect_requires_symbol() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["collect"])
    assert result.exit_code != 0
    assert "Please provide at least one symbol" in result.output


def test_collect_yfinance_unsupported() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["collect", "--symbol", "AAPL", "--source", "yfinance"])
    assert result.exit_code != 0
    assert "is not supported by `collect` yet" in result.output


def test_collect_sharadar_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SHARADAR_API_KEY", raising=False)
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["collect", "--symbol", "MSFT", "--source", "sharadar"])
    assert result.exit_code != 0
    assert "Environment variable SHARADAR_API_KEY is not set." in result.output


def test_constants_env_only_table(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHARADAR_API_KEY", "secret")
    runner = CliRunner()
    result = runner.invoke(cli_mod.main, ["constants", "--env-only", "--format", "table"])
    assert result.exit_code == 0
    assert "SHARADAR_API_KEY" in result.output
    assert "<redacted>" in result.output


def test_constants_sections_table() -> None:
    runner = CliRunner()
    r1 = runner.invoke(cli_mod.main, ["constants", "--section", "flow", "--format", "table"])
    assert r1.exit_code == 0
    assert "[flow]" in r1.output
    r2 = runner.invoke(cli_mod.main, ["constants", "--section", "writers", "--format", "table"])
    assert r2.exit_code == 0
    assert "[writers]" in r2.output
    assert "WRITER_DUCKDB_MAX_WORKERS" in r2.output
    # status also prints total size
    r3 = runner.invoke(cli_mod.main, ["status"])
    assert r3.exit_code == 0
    assert "Total Data Size" in r3.output


def test_constants_yfinance_sharadar_json() -> None:
    runner = CliRunner()
    r1 = runner.invoke(cli_mod.main, ["constants", "--section", "yfinance", "--format", "json"])
    assert r1.exit_code == 0
    assert "PRICE_BATCH_SIZE" in r1.output
    r2 = runner.invoke(cli_mod.main, ["constants", "--section", "sharadar", "--format", "json"])
    assert r2.exit_code == 0
    assert "MAX_ROWS_PER_REQUEST" in r2.output


def test_constants_env_only_ignores_non_auth_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_CONCURRENCY", "16")
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "flow", "--format", "table", "--env-only"])
    assert res.exit_code == 0
    assert "[flow]" not in res.output
    assert "VF_CONCURRENCY" not in res.output


def test_constants_env_only_global_skips_removed_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "15")
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "global", "--format", "table", "--env-only"])
    assert res.exit_code == 0
    assert "[global]" not in res.output
    assert "HTTP_TIMEOUT_S" not in res.output


def test_constants_queue_table() -> None:
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "queue", "--format", "table"])
    assert res.exit_code == 0
    # Basic keys rendered in table format
    assert "[queue]" in res.output
    assert "QUEUE_DEFAULT" in res.output


def test_constants_queue_json() -> None:
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "queue", "--format", "json"])
    assert res.exit_code == 0
    assert '"QUEUE_MIN"' in res.output
    assert '"QUEUE_MAX"' in res.output


def test_constants_writers_json() -> None:
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "writers", "--format", "json"])
    assert res.exit_code == 0
    assert '"WRITER_DUCKDB_MAX_WORKERS"' in res.output
    assert '"WAL_AUTOCHECKPOINT_LIMIT"' in res.output


def test_recover_table_specified_but_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Create DLQ structure with a different table than requested
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq = base / "cache" / "dlq" / "existing_tbl"
    dlq.mkdir(parents=True, exist_ok=True)
    (dlq / "batch_1.ipc").write_bytes(b"x")
    runner = CliRunner()
    res = runner.invoke(
        cli_mod.main,
        ["recover", "--dir", str(base / "cache" / "dlq"), "--table", "missing_tbl", "--dry-run"],
    )
    assert res.exit_code == 0
    # Fallback summary path when specified tables not found
    assert "✅ Recover summary:" in res.output
    assert "tables=0" in res.output


def test_constants_env_only_keeps_auth_env_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "15")
    monkeypatch.setenv("SHARADAR_API_KEY", "secret")
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "global", "--format", "table", "--env-only"])
    assert res.exit_code == 0
    assert "SHARADAR_API_KEY" in res.output
    assert "VF_HTTP_TIMEOUT_S" not in res.output


def test_constants_writers_table() -> None:
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "writers", "--format", "table"])
    assert res.exit_code == 0
    assert "[writers]" in res.output
    assert "WRITER_DUCKDB_MAX_WORKERS" in res.output
    assert "WAL_AUTOCHECKPOINT_LIMIT" in res.output


def test_recover_missing_dir_errors(tmp_path: Path) -> None:
    # Non-existent path should raise ClickException routed to non-zero exit
    missing = tmp_path / "no_such_dlq"
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["recover", "--dir", str(missing)])
    assert res.exit_code != 0
    assert "DLQ directory not found" in res.output


def test_recover_empty_dir_no_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq = base / "cache" / "dlq"
    dlq.mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["recover", "--dir", str(dlq), "--dry-run"])
    assert res.exit_code == 0
    assert "No tables selected or found under DLQ." in res.output


def test_constants_all_json_has_sections() -> None:
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "all", "--format", "json"])
    assert res.exit_code == 0
    assert '"global"' in res.output
    assert '"flow"' in res.output


def test_constants_json_env_overrides_included(monkeypatch: pytest.MonkeyPatch) -> None:
    import json as _json

    monkeypatch.setenv("SHARADAR_API_KEY", "secret")
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["constants", "--section", "global", "--format", "json"])
    assert res.exit_code == 0
    data = _json.loads(res.output)
    assert "env_overrides" in data
    assert data["env_overrides"]["SHARADAR_API_KEY"] == "<redacted>"


def test_collect_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import httpx

    class _BrokenCtx:
        async def __aenter__(self):
            raise httpx.RequestError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _create_client(**kwargs):  # type: ignore[no-untyped-def]
        return _BrokenCtx()

    monkeypatch.setenv("SHARADAR_API_KEY", "x")
    monkeypatch.setattr(cli_mod, "create_client", _create_client, raising=True)
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["collect", "--symbol", "AAPL", "--source", "sharadar"])
    assert res.exit_code != 0
    assert "network request failed" in res.output


def test_collect_value_error_converted(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BrokenCtx:
        async def __aenter__(self):
            raise ValueError("oops")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setenv("SHARADAR_API_KEY", "x")
    monkeypatch.setattr(cli_mod, "create_client", lambda **_: _BrokenCtx(), raising=True)
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["collect", "--symbol", "AAPL", "--source", "sharadar"])
    assert res.exit_code != 0
    assert "Collection error: oops" in res.output


# ===== Recover CLI tests merged from test_cli_recover_args.py =====


def _make_dlq(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, table: str, rows: int = 3) -> Path:
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq_tbl = get_cache_dir() / "dlq" / table
    dlq_tbl.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({"a": list(range(rows))})
    fpath = dlq_tbl / "batch_12345.ipc"
    with fpath.open("wb") as fh:
        df.write_ipc(fh)
    return dlq_tbl


def test_recover_dedup_tables_argument_parsing(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _make_dlq(tmp_path, monkeypatch, "t1")
    runner = CliRunner()
    res = runner.invoke(
        cli_mod.main,
        [
            "recover",
            "--dir",
            str(get_cache_dir() / "dlq"),
            "--table",
            "t1",
            "--table",
            "t1",
            "--dry-run",
        ],
    )
    assert res.exit_code == 0
    assert "✅ Recover summary:" in res.output


def test_recover_env_db_empty_rejected(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _make_dlq(tmp_path, monkeypatch, "t2")
    monkeypatch.setenv("VF_RECOVER_DB", "")
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["recover", "--dir", str(get_cache_dir() / "dlq")])
    assert res.exit_code != 0
    assert "Missing target DB" in str(res.output)


def test_recover_report_schema(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _make_dlq(tmp_path, monkeypatch, "t3", rows=4)
    runner = CliRunner()
    report_path = tmp_path / "report.json"
    res = runner.invoke(
        cli_mod.main,
        [
            "recover",
            "--dir",
            str(get_cache_dir() / "dlq"),
            "--table",
            "t3",
            "--dry-run",
            "--report",
            str(report_path),
        ],
    )
    assert res.exit_code == 0
    data = json.loads(report_path.read_text())
    assert "tables" in data
    assert "errors" in data
    assert "error_counts" in data
    t = data["tables"].get("t3")
    assert t is not None
    assert "files_scanned" in t
    assert "rows_scanned" in t
    assert "rows_written" in t
    assert "details" in t


def test_recover_progress_and_verbose(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _make_dlq(tmp_path, monkeypatch, "t4", rows=2)
    runner = CliRunner()
    res = runner.invoke(
        cli_mod.main,
        [
            "recover",
            "--dir",
            str(get_cache_dir() / "dlq"),
            "--table",
            "t4",
            "--dry-run",
            "--progress",
            "--verbose",
        ],
    )
    assert res.exit_code == 0
    assert "[scan] t4 batch_12345.ipc" in res.output
    assert "[detail] t4 file=" in res.output


def test_recover_strict_with_read_error(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq_tbl = get_cache_dir() / "dlq" / "t_err"
    dlq_tbl.mkdir(parents=True, exist_ok=True)
    bad = dlq_tbl / "batch_bad.ipc"
    bad.write_bytes(b"not-ipc")
    runner = CliRunner()
    res = runner.invoke(
        cli_mod.main,
        [
            "recover",
            "--dir",
            str(get_cache_dir() / "dlq"),
            "--table",
            "t_err",
            "--dry-run",
            "--strict",
        ],
    )
    assert res.exit_code != 0
    assert "Errors encountered" in res.output
