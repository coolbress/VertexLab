from __future__ import annotations

import duckdb
import polars as pl
import pytest
from datetime import datetime, timezone, date
from pathlib import Path

from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.core.config import FramePacket


@pytest.mark.asyncio
async def test_schema_evolution_adds_missing_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "evolve.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        table = "test_table"
        df1 = pl.DataFrame({"provider": ["t"], "ticker": ["A"], "date": [date.today()], "val": [1]})
        p1 = FramePacket(provider="t", table=table, frame=df1, observed_at=datetime.now(timezone.utc))
        res1 = await writer.write(p1)
        assert res1.rows == 1

        df2 = pl.DataFrame({"provider": ["t"], "ticker": ["A"], "date": [date.today()], "val": [2], "val2": [3]})
        p2 = FramePacket(provider="t", table=table, frame=df2, observed_at=datetime.now(timezone.utc))
        res2 = await writer.write(p2)
        assert res2.rows == 1

        with duckdb.connect(str(db_path)) as conn:
            cols = {row[0] for row in conn.execute('DESCRIBE "test_table"').fetchall()}
            assert "val2" in cols
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_write_bulk_empty_returns_empty(tmp_path: Path) -> None:
    db_path = tmp_path / "bulk.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        out = await writer.write_bulk([])
        assert out == []
    finally:
        await writer.close()
