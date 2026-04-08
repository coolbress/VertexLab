from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from click.testing import CliRunner
import duckdb
import polars as pl
import pytest

from vertex_forager.cli import main
from vertex_forager.core.checkpoint import list_dlq_entries, register_dlq_entry
from vertex_forager.utils import get_cache_dir


def _write_price_ipc(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "provider": ["yfinance"],
            "ticker": ["AAPL"],
            "date": [date(2024, 1, 1)],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "volume": [1000.0],
            "fetched_at": [datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)],
        }
    ).write_ipc(path)


@pytest.mark.integration
def test_dlq_replay_integration_uses_stored_output_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    output_db = tmp_path / "out.duckdb"
    batch_path = get_cache_dir() / "dlq" / "yfinance_price" / "batch_1.ipc"
    _write_price_ipc(batch_path)
    register_dlq_entry(
        path=batch_path,
        table="yfinance_price",
        provider="yfinance",
        row_count=1,
        output_uri=f"duckdb://{output_db}",
    )

    result = CliRunner().invoke(main, ["dlq", "replay", "--table", "yfinance_price"])
    assert result.exit_code == 0
    assert "replayed=1" in result.output

    with duckdb.connect(str(output_db)) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM yfinance_price").fetchone()
        assert rows == (1,)

    entries = list_dlq_entries(table="yfinance_price", status="recovered")
    assert len(entries) == 1


@pytest.mark.integration
def test_dlq_replay_integration_dry_run_and_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "app"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    stored_db = tmp_path / "stored.duckdb"
    override_db = tmp_path / "override.duckdb"
    batch_path = get_cache_dir() / "dlq" / "yfinance_price" / "batch_2.ipc"
    _write_price_ipc(batch_path)
    register_dlq_entry(
        path=batch_path,
        table="yfinance_price",
        provider="yfinance",
        row_count=1,
        output_uri=f"duckdb://{stored_db}",
    )

    dry_run = CliRunner().invoke(main, ["dlq", "replay", "--table", "yfinance_price", "--dry-run"])
    assert dry_run.exit_code == 0
    assert "replayed=1" in dry_run.output
    assert not stored_db.exists()
    assert not override_db.exists()

    replay = CliRunner().invoke(
        main,
        ["dlq", "replay", "--table", "yfinance_price", "--output", f"duckdb://{override_db}"],
    )
    assert replay.exit_code == 0
    assert not stored_db.exists()
    with duckdb.connect(str(override_db)) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM yfinance_price").fetchone()
        assert rows == (1,)
