import duckdb
import polars as pl
import pytest
from datetime import datetime, timezone, date

from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.core.config import FramePacket
from vertex_forager.exceptions import InputError


@pytest.mark.asyncio
async def test_invalid_identifier_raises_input_error(tmp_path):
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
async def test_reserved_word_identifier_ok(tmp_path):
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
        conn = duckdb.connect(str(db_path))
        out = conn.execute('SELECT count(*) FROM "select"').fetchone()[0]
        assert out == 1
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_upsert_conflict_updates_value(tmp_path):
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
        await writer.write(p1)

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
        await writer.write(p2)

        conn = duckdb.connect(str(db_path))
        val = conn.execute(
            'SELECT close FROM "yfinance_price" WHERE provider=? AND ticker=? AND date=?',
            ["yfinance", "AAPL", today],
        ).fetchone()[0]
        assert val == 110.0
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_identifier_edge_cases_invalid(tmp_path):
    db_path = tmp_path / "edge.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"x": [1]})
        bad_names = [
            '"bad"',  # double quotes unescaped in raw name
            "'bad'",  # single quotes
            "bad`name",  # backtick
            "bad\x00name",  # null byte
            "bad\nname",  # control char
            " bad",  # leading whitespace
            "bad ",  # trailing whitespace
            "",  # empty
        ]
        for name in bad_names:
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


@pytest.mark.asyncio
async def test_identifier_explicit_quoted_select_succeeds(tmp_path):
    db_path = tmp_path / "quoted.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"x": [2]})
        packet = FramePacket(
            provider="test",
            table='select',  # writer will quote internally
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        res = await writer.write(packet)
        assert res.rows == 1
        conn = duckdb.connect(str(db_path))
        out = conn.execute('SELECT count(*) FROM "select"').fetchone()[0]
        assert out == 1
    finally:
        await writer.close()
