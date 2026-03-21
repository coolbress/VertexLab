from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.exceptions import PrimaryKeyMissingError
from vertex_forager.writers.duckdb import DuckDBWriter


@pytest.mark.asyncio
async def test_duckdb_writer_creates_unique_index_and_upserts(tmp_path: Path) -> None:
    db_path = tmp_path / "test_db.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        # Initial insert for yfinance_price with full PK
        df1 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["AAPL"],
                "date": [date(2020, 1, 1)],
                "close": [100.0],
            }
        )
        pkt1 = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df1,
            observed_at=datetime.now(timezone.utc),
        )
        await writer.write(pkt1)

        # Second write with same PK but different close to exercise ON CONFLICT DO UPDATE
        df2 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["AAPL"],
                "date": [date(2020, 1, 1)],
                "close": [111.0],
            }
        )
        pkt2 = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df2,
            observed_at=datetime.now(timezone.utc),
        )
        await writer.write(pkt2)
    finally:
        # Ensure writer resources are cleaned up before direct DB access
        await writer.close()

    con = duckdb.connect(str(db_path))
    try:
        # Verify single row and updated value
        rows = con.execute("SELECT count(*) FROM yfinance_price").fetchone()[0]
        assert rows == 1
        val = con.execute("SELECT close FROM yfinance_price").fetchone()[0]
        assert pytest.approx(val) == 111.0

        # Verify unique index existence via catalog if available
        try:
            cnt = con.execute(
                "SELECT COUNT(*) FROM duckdb_indexes() "
                "WHERE table_name='yfinance_price' AND index_name='idx_yfinance_price_pk'"
            ).fetchone()[0]
            assert cnt >= 1
        except duckdb.Error as e:
            msg = str(e).lower()
            if "duckdb_indexes" in msg and ("does not exist" in msg or "no function matches" in msg):
                pytest.skip("duckdb_indexes() not available on this DuckDB build")
            raise
    finally:
        con.close()


@pytest.mark.asyncio
async def test_duckdb_writer_schema_evolution_adds_column(tmp_path: Path) -> None:
    db_path = tmp_path / "evolve.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        df1 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["MSFT"],
                "date": [date(2021, 5, 4)],
                "close": [250.0],
            }
        )
        pkt1 = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df1,
            observed_at=datetime.now(timezone.utc),
        )
        await writer.write(pkt1)

        # Add a new column not present before -> triggers _sync_schema
        df2 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["MSFT"],
                "date": [date(2021, 5, 4)],
                "close": [255.0],
                "extra_col": [1],
            }
        )
        pkt2 = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df2,
            observed_at=datetime.now(timezone.utc),
        )
        await writer.write(pkt2)
    finally:
        # Ensure writer is closed before direct inspection
        await writer.close()

    con = duckdb.connect(str(db_path))
    try:
        cols = {r[0] for r in con.execute("DESCRIBE yfinance_price").fetchall()}
        assert "extra_col" in cols
    finally:
        con.close()


@pytest.mark.asyncio
async def test_duckdb_writer_missing_pk_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "pk_missing.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        # Missing 'date' from PK ("provider","ticker","date")
        df = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["AAPL"],
                "close": [123.0],
            }
        )
        pkt = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        with pytest.raises(PrimaryKeyMissingError):
            await writer.write(pkt)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_duckdb_writer_upsert_do_nothing_only_pk(tmp_path: Path) -> None:
    db_path = tmp_path / "pk_only.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        df = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["IBM"],
                "date": [date(2022, 2, 2)],
            }
        )
        pkt = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        # First write creates table and inserts PK-only row (DO NOTHING on conflict not triggered)
        await writer.write(pkt)
        # Second write with identical PK and PK-only columns should DO NOTHING on conflict
        await writer.write(pkt)
    finally:
        # Close writer prior to direct DB readback
        await writer.close()

    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("SELECT count(*) FROM yfinance_price").fetchone()[0]
        assert rows == 1
    finally:
        con.close()
