from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.exceptions import (
    InputError,
    PrimaryKeyMissingError,
    PrimaryKeyNullError,
    SchemaMapError,
)
from vertex_forager.writers.duckdb import DuckDBWriter


@pytest.mark.asyncio
async def test_invalid_identifier_raises_input_error(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"provider": ["t"], "ticker": ["A"], "date": [date.today()]})
        packet = FramePacket(provider="test", table="bad-name", frame=df, observed_at=datetime.now(timezone.utc))
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
        packet = FramePacket(provider="test", table="select", frame=df, observed_at=datetime.now(timezone.utc))
        res = await writer.write(packet)
        assert res.rows == 1
        with duckdb.connect(str(db_path)) as conn:
            row = conn.execute('SELECT count(*) FROM "select"').fetchone()
            assert row
            assert row[0] == 1
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_upsert_conflict_updates_value(tmp_path: Path) -> None:
    db_path = tmp_path / "upsert.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        table = "yfinance_price"
        today = date.today()
        df1 = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "date": [today], "close": [100.0]})
        p1 = FramePacket(provider="yfinance", table=table, frame=df1, observed_at=datetime.now(timezone.utc))
        await writer.write(p1)
        df2 = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "date": [today], "close": [110.0]})
        p2 = FramePacket(provider="yfinance", table=table, frame=df2, observed_at=datetime.now(timezone.utc))
        await writer.write(p2)
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
@pytest.mark.parametrize("name", ['"bad"', "'bad'", "bad`name", "bad\x00name", "bad\nname", " bad", "bad ", ""])
async def test_identifier_edge_cases_invalid(tmp_path: Path, name: str) -> None:
    db_path = tmp_path / "edge.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        df = pl.DataFrame({"x": [1]})
        packet = FramePacket(provider="test", table=name, frame=df, observed_at=datetime.now(timezone.utc))
        with pytest.raises(InputError):
            await writer.write(packet)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_schema_evolution_type_mapping_timestamptz_and_hugeint(tmp_path: Path) -> None:
    db_path = tmp_path / "types.duckdb"
    writer = DuckDBWriter(str(db_path))
    try:
        table = "tmapping"
        df1 = pl.DataFrame({"provider": ["t"], "ticker": ["A"], "date": [date.today()]})
        await writer.write(FramePacket(provider="t", table=table, frame=df1, observed_at=datetime.now(timezone.utc)))
        u64 = pl.Series("u64", [2], dtype=pl.UInt64)
        df2 = pl.DataFrame(
            {"provider": ["t"], "ticker": ["A"], "date": [date.today()], "tz": [datetime.now(timezone.utc)]}
        ).with_columns([u64])
        await writer.write(FramePacket(provider="t", table=table, frame=df2, observed_at=datetime.now(timezone.utc)))
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute('DESCRIBE "tmapping"').fetchall()
            types = {r[0]: r[1] for r in rows}
            assert "WITH TIME ZONE" in (types.get("tz") or "").upper()
            t_u64 = str(types.get("u64") or "").upper()
            assert t_u64 == "UBIGINT"
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_duckdb_writer_creates_unique_index_and_upserts(tmp_path: Path) -> None:
    db_path = tmp_path / "test_db.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        df1 = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "date": [date(2020, 1, 1)], "close": [100.0]})
        await writer.write(
            FramePacket(provider="yfinance", table="yfinance_price", frame=df1, observed_at=datetime.now(timezone.utc))
        )
        df2 = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "date": [date(2020, 1, 1)], "close": [111.0]})
        await writer.write(
            FramePacket(provider="yfinance", table="yfinance_price", frame=df2, observed_at=datetime.now(timezone.utc))
        )
    finally:
        await writer.close()
    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("SELECT count(*) FROM yfinance_price").fetchone()[0]
        assert rows == 1
        val = con.execute("SELECT close FROM yfinance_price").fetchone()[0]
        assert val == pytest.approx(111.0)
    finally:
        con.close()


@pytest.mark.asyncio
async def test_duckdb_writer_schema_evolution_adds_column(tmp_path: Path) -> None:
    db_path = tmp_path / "evolve.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        df1 = pl.DataFrame({"provider": ["yfinance"], "ticker": ["MSFT"], "date": [date(2021, 5, 4)], "close": [250.0]})
        await writer.write(
            FramePacket(provider="yfinance", table="yfinance_price", frame=df1, observed_at=datetime.now(timezone.utc))
        )
        df2 = pl.DataFrame(
            {
                "provider": ["yfinance"],
                "ticker": ["MSFT"],
                "date": [date(2021, 5, 4)],
                "close": [255.0],
                "extra_col": [1],
            }
        )
        await writer.write(
            FramePacket(provider="yfinance", table="yfinance_price", frame=df2, observed_at=datetime.now(timezone.utc))
        )
    finally:
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
        df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "close": [123.0]})
        with pytest.raises(PrimaryKeyMissingError):
            await writer.write(
                FramePacket(
                    provider="yfinance",
                    table="yfinance_price",
                    frame=df,
                    observed_at=datetime.now(timezone.utc),
                )
            )
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_duckdb_writer_upsert_do_nothing_only_pk(tmp_path: Path) -> None:
    db_path = tmp_path / "pk_only.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["IBM"], "date": [date(2022, 2, 2)]})
        pkt = FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=df,
            observed_at=datetime.now(timezone.utc),
        )
        await writer.write(pkt)
        await writer.write(pkt)
    finally:
        await writer.close()
    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("SELECT count(*) FROM yfinance_price").fetchone()[0]
        assert rows == 1
    finally:
        con.close()


@pytest.mark.asyncio
async def test_map_polars_types() -> None:
    w = DuckDBWriter(":memory:")
    try:
        assert w._map_polars_type_to_sql(pl.Int8) == "TINYINT"
        assert w._map_polars_type_to_sql(pl.Int16) == "SMALLINT"
        assert w._map_polars_type_to_sql(pl.Int32) == "INTEGER"
        assert w._map_polars_type_to_sql(pl.Int64) == "BIGINT"
        assert w._map_polars_type_to_sql(pl.UInt8) == "UTINYINT"
        assert w._map_polars_type_to_sql(pl.UInt16) == "USMALLINT"
        assert w._map_polars_type_to_sql(pl.UInt32) == "UINTEGER"
        assert w._map_polars_type_to_sql(pl.UInt64) == "UBIGINT"
        assert w._map_polars_type_to_sql(pl.Float32) == "FLOAT"
        assert w._map_polars_type_to_sql(pl.Float64) == "DOUBLE"
        assert w._map_polars_type_to_sql(pl.Boolean) == "BOOLEAN"
        assert w._map_polars_type_to_sql(pl.Date) == "DATE"
        tz_dtype = pl.Datetime(time_zone="UTC")
        assert w._map_polars_type_to_sql(tz_dtype) == "TIMESTAMPTZ"
        assert w._map_polars_type_to_sql(pl.Datetime) == "TIMESTAMP"
        assert w._map_polars_type_to_sql(pl.Duration) == "INTERVAL"
        assert w._map_polars_type_to_sql(pl.String) == "VARCHAR"
        assert w._map_polars_type_to_sql(pl.Categorical) == "VARCHAR"

        # Test nested types (List and Struct)
        assert w._map_polars_type_to_sql(pl.List(pl.Float64)) == "DOUBLE[]"
        assert w._map_polars_type_to_sql(pl.Struct({"id": pl.Int64, "name": pl.String})) == (
            'STRUCT("id" BIGINT, "name" VARCHAR)'
        )
        # Deeply nested
        nested_type = pl.List(pl.Struct({"a": pl.Int32, "b": pl.List(pl.String)}))
        assert w._map_polars_type_to_sql(nested_type) == 'STRUCT("a" INTEGER, "b" VARCHAR[])[]'
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_identifier_validation_and_quoting() -> None:
    w = DuckDBWriter(":memory:")
    try:
        assert w._quote_identifier("valid_name_123") == '"valid_name_123"'
        with pytest.raises(InputError):
            w._quote_identifier("bad-name")
        with pytest.raises(InputError):
            w._quote_identifier("bad\x07name")
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_ensure_table_exists_missing_pk_skips_index(tmp_path: Path) -> None:
    db_path = tmp_path / "no_pk_index.duckdb"
    w = DuckDBWriter(db_path)
    try:
        con = w._get_connection()
        df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["ZZZ"], "close": [1.0]})
        w._ensure_table_exists(con, "yfinance_price", df)
        try:
            cnt = con.execute(
                "SELECT COUNT(*) FROM duckdb_indexes() "
                "WHERE table_name='yfinance_price' AND index_name='idx_yfinance_price_pk'"
            ).fetchone()[0]
            assert cnt == 0
        except duckdb.Error as e:
            msg = str(e).lower()
            if "duckdb_indexes" in msg and ("does not exist" in msg or "no function matches" in msg):
                pytest.skip("duckdb_indexes() not available on this DuckDB build")
            raise
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_write_pk_null_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "pk_null.duckdb"
    w = DuckDBWriter(db_path)
    try:
        df = pl.DataFrame(
            {"provider": ["yfinance"], "ticker": ["AAPL"], "date": [None], "close": [1.0]}
        ).with_columns(pl.col("date").cast(pl.Date))
        pkt = FramePacket(provider="yfinance", table="yfinance_price", frame=df, observed_at=datetime.now(timezone.utc))
        with pytest.raises(PrimaryKeyNullError):
            await w.write(pkt)
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_schema_map_strict_mode() -> None:
    # strict=False (default) -> fallback to VARCHAR, increments counter
    w_non_strict = DuckDBWriter(":memory:", strict=False)
    try:
        assert w_non_strict.unresolved_type_count == 0
        # Passing an unknown object type
        assert w_non_strict._map_polars_type_to_sql(pl.Object) == "VARCHAR"
        assert w_non_strict.unresolved_type_count == 1
    finally:
        await w_non_strict.close()

    # strict=True -> raises SchemaMapError
    w_strict = DuckDBWriter(":memory:", strict=True)
    try:
        assert w_strict.unresolved_type_count == 0
        with pytest.raises(SchemaMapError, match="Unsupported Polars type"):
            w_strict._map_polars_type_to_sql(pl.Object)
        assert w_strict.unresolved_type_count == 1
    finally:
        await w_strict.close()


@pytest.mark.asyncio
async def test_duckdb_writer_nested_types_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "nested.duckdb"
    writer = DuckDBWriter(db_path)
    try:
        # Create a DataFrame with nested List and Struct types
        df = pl.DataFrame(
            {
                "provider": ["test"],
                "ticker": ["NEST"],
                "date": [date(2023, 1, 1)],
                "tags": [["tech", "ai"]],
                "metrics": [{"score": 9.5, "active": True}],
                "history": [[{"val": 1.0}, {"val": 2.0}]],
            },
            schema={
                "provider": pl.String,
                "ticker": pl.String,
                "date": pl.Date,
                "tags": pl.List(pl.String),
                "metrics": pl.Struct({"score": pl.Float64, "active": pl.Boolean}),
                "history": pl.List(pl.Struct({"val": pl.Float64})),
            }
        )
        packet = FramePacket(
            provider="test",
            table="nested_data",
            frame=df,
            observed_at=datetime.now(timezone.utc)
        )
        await writer.write(packet)

        # Read back and verify types
        with duckdb.connect(str(db_path)) as conn:
            # Check schema
            schema_info = conn.execute("DESCRIBE nested_data").fetchall()
            type_map = {row[0]: row[1] for row in schema_info}

            assert type_map["tags"] == "VARCHAR[]"
            assert type_map["metrics"] == "STRUCT(score DOUBLE, active BOOLEAN)"
            assert type_map["history"] == "STRUCT(val DOUBLE)[]"

            # Check data via native SQL
            res = conn.execute("SELECT metrics.score, tags[1], history[2].val FROM nested_data").fetchone()
            assert res == (9.5, "tech", 2.0)
    finally:
        await writer.close()
