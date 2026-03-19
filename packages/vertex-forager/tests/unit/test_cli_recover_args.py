import json
from pathlib import Path

from click.testing import CliRunner
import polars as pl
import pytest

from vertex_forager.cli import main
from vertex_forager.utils import get_cache_dir


def _make_dlq(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, table: str, rows: int = 3
) -> Path:
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq_tbl = get_cache_dir() / "dlq" / table
    dlq_tbl.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({"a": list(range(rows))})
    fpath = dlq_tbl / "batch_12345.ipc"
    with fpath.open("wb") as fh:
        df.write_ipc(fh)
    return dlq_tbl


def test_recover_dedup_tables_argument_parsing(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    _make_dlq(tmp_path, monkeypatch, "t1")
    runner = CliRunner()
    res = runner.invoke(
        main,
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
    res = runner.invoke(main, ["recover", "--dir", str(get_cache_dir() / "dlq")])
    assert res.exit_code != 0
    assert "Missing target DB" in str(res.output)


def test_recover_report_schema(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _make_dlq(tmp_path, monkeypatch, "t3", rows=4)
    runner = CliRunner()
    report_path = tmp_path / "report.json"
    res = runner.invoke(
        main,
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


def test_recover_progress_and_verbose(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    _make_dlq(tmp_path, monkeypatch, "t4", rows=2)
    runner = CliRunner()
    res = runner.invoke(
        main,
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
        main,
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
