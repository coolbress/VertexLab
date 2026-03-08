import json
from pathlib import Path
import polars as pl
import pytest
from click.testing import CliRunner
from vertex_forager.cli import main
from vertex_forager.utils import get_cache_dir


def _make_ipc(dir_path: Path, rows: int) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({"x": list(range(rows))})
    f = dir_path / "batch_100.ipc"
    with f.open("wb") as fh:
        df.write_ipc(fh)
    return f


@pytest.mark.integration
def test_recover_integration_dry_run_and_write_delete(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq_root = get_cache_dir() / "dlq"
    t_ok_dir = dlq_root / "t_ok"
    t_err_dir = dlq_root / "t_err"
    ok_file = _make_ipc(t_ok_dir, rows=3)
    bad_file = t_err_dir / "batch_bad.ipc"
    bad_file.parent.mkdir(parents=True, exist_ok=True)
    bad_file.write_bytes(b"not-ipc")

    runner = CliRunner()
    report_dry = tmp_path / "dry.json"
    res_dry = runner.invoke(
        main,
        [
            "recover",
            "--dir",
            str(dlq_root),
            "--table",
            "t_ok",
            "--table",
            "t_err",
            "--dry-run",
            "--report",
            str(report_dry),
            "--progress",
            "--verbose",
        ],
    )
    assert res_dry.exit_code == 0
    data_dry = json.loads(report_dry.read_text())
    assert "tables" in data_dry and "error_counts" in data_dry
    t_ok = data_dry["tables"]["t_ok"]
    t_err = data_dry["tables"]["t_err"]
    assert t_ok["files_scanned"] == 1 and t_ok["rows_scanned"] == 3
    assert any(d.get("status") == "scanned" for d in t_ok["details"])
    assert t_err["files_scanned"] == 1
    assert data_dry["error_counts"].get("RecoverFail", 0) >= 1

    db_path = tmp_path / "out.duckdb"
    report_write = tmp_path / "write.json"
    res_write = runner.invoke(
        main,
        [
            "recover",
            "--dir",
            str(dlq_root),
            "--table",
            "t_ok",
            "--db",
            str(db_path),
            "--delete-on-success",
            "--report",
            str(report_write),
        ],
    )
    assert res_write.exit_code == 0
    data_write = json.loads(report_write.read_text())
    assert data_write["tables"]["t_ok"]["rows_written"] == 3
    assert not ok_file.exists()


@pytest.mark.integration
def test_recover_integration_strict_on_error(tmp_path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    dlq_root = get_cache_dir() / "dlq"
    t_err_dir = dlq_root / "t_err_only"
    bad_file = t_err_dir / "batch_bad.ipc"
    bad_file.parent.mkdir(parents=True, exist_ok=True)
    bad_file.write_bytes(b"not-ipc")

    runner = CliRunner()
    res = runner.invoke(
        main,
        [
            "recover",
            "--dir",
            str(dlq_root),
            "--table",
            "t_err_only",
            "--dry-run",
            "--strict",
        ],
    )
    assert res.exit_code != 0
    assert "Errors encountered" in res.output
