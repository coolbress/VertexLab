from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest
from vertex_forager.core.config import FramePacket
from vertex_forager.writers.duckdb import DuckDBWriter


@pytest.mark.asyncio
async def test_schema_evolution_type_mapping_timestamptz_and_hugeint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "types.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        table = "tmapping"
        df1 = pl.DataFrame(
            {"provider": ["t"], "ticker": ["A"], "date": [date.today()]}
        )
        p1 = FramePacket(
            provider="t",
            table=table,
            frame=df1,
            observed_at=datetime.now(timezone.utc),
        )
        res1 = await writer.write(p1)
        assert res1.rows == 1

        u64 = pl.Series("u64", [2], dtype=pl.UInt64)
        df2 = pl.DataFrame(
            {
                "provider": ["t"],
                "ticker": ["A"],
                "date": [date.today()],
                "tz": [datetime.now(timezone.utc)],
            }
        ).with_columns([u64])
        p2 = FramePacket(
            provider="t",
            table=table,
            frame=df2,
            observed_at=datetime.now(timezone.utc),
        )
        res2 = await writer.write(p2)
        assert res2.rows == 1

        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute('DESCRIBE "tmapping"').fetchall()
            types = {r[0]: r[1] for r in rows}
            assert types.get("tz") in ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")
            t_u64 = str(types.get("u64") or "").upper()
            assert t_u64 in ("HUGEINT", "UBIGINT", "BIGINT")
    finally:
        await writer.close()
