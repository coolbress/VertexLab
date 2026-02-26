import duckdb
import polars as pl
import pytest
from datetime import datetime, timezone, date
from pathlib import Path

from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.core.config import FramePacket
from vertex_forager.exceptions import InputError


@pytest.mark.asyncio
async def test_invalid_identifier_raises_input_error(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"provider": ["t"], "ticker": ["A"], "date": [date.today()]})
        packet = FramePacket(
            provider="test",
            table="bad-name",
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        with pytest.raises(InputError):
            await writer.write(packet)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_reserved_word_identifier_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "test2.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"x": [1]})
        packet = FramePacket(
            provider="test",
            table="select",
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        res = await writer.write(packet)
        assert res.rows == 1
        with duckdb.connect(str(db_path)) as conn:
            out = conn.execute('SELECT count(*) FROM "select"').fetchone()[0]
            assert out == 1
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_upsert_conflict_updates_value(tmp_path: Path) -> None:
    db_path = tmp_path / "upsert.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        table = "yfinance_price"
        today = date.today()

        df1 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["AAPL"],
                "date": [today],
                "close": [100.0],
            }
        )
        p1 = FramePacket(
            provider="yfinance",
            table=table,
            frame=df1,
            observed_at=datetime.now(timezone.utc),
        )
        result1 = await writer.write(p1)
        assert result1.rows == 1

        df2 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["AAPL"],
                "date": [today],
                "close": [110.0],
            }
        )
        p2 = FramePacket(
            provider="yfinance",
            table=table,
            frame=df2,
            observed_at=datetime.now(timezone.utc),
        )
        result2 = await writer.write(p2)
        assert result2.rows == 1

        with duckdb.connect(str(db_path)) as conn:
            val = conn.execute(
                'SELECT close FROM "yfinance_price" WHERE provider=? AND ticker=? AND date=?',
                ["yfinance", "AAPL", today],
            ).fetchone()[0]
            cnt = conn.execute(
                'SELECT count(*) FROM "yfinance_price" WHERE provider=? AND ticker=? AND date=?',
                ["yfinance", "AAPL", today],
            ).fetchone()[0]
            assert val == 110.0
            assert cnt == 1
    finally:
        await writer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    [
        '"bad"',  # double quotes unescaped in raw name
        "'bad'",  # single quotes
        "bad`name",  # backtick
        "bad\x00name",  # null byte
        "bad\nname",  # control char
        " bad",  # leading whitespace
        "bad ",  # trailing whitespace
        "",  # empty
    ],
)
async def test_identifier_edge_cases_invalid(tmp_path: Path, name: str) -> None:
    db_path = tmp_path / "edge.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"x": [1]})
        packet = FramePacket(
            provider="test",
            table=name,
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        with pytest.raises(InputError):
            await writer.write(packet)
    finally:
        await writer.close()


# Removed duplicate reserved-word test; covered by test_reserved_word_identifier_ok.
