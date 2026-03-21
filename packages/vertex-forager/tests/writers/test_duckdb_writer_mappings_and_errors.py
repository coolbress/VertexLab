from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.exceptions import InputError, PrimaryKeyNullError
from vertex_forager.writers.duckdb import DuckDBWriter


def test_map_polars_types() -> None:
    w = DuckDBWriter(":memory:")
    assert w._map_polars_type_to_sql(pl.Int8) == "TINYINT"
    assert w._map_polars_type_to_sql(pl.Int16) == "SMALLINT"
    assert w._map_polars_type_to_sql(pl.Int32) == "INTEGER"
    assert w._map_polars_type_to_sql(pl.Int64) == "BIGINT"
    assert w._map_polars_type_to_sql(pl.UInt8) == "SMALLINT"
    assert w._map_polars_type_to_sql(pl.UInt16) == "INTEGER"
    assert w._map_polars_type_to_sql(pl.UInt32) == "BIGINT"
    assert w._map_polars_type_to_sql(pl.UInt64) == "HUGEINT"
    assert w._map_polars_type_to_sql(pl.Float32) == "FLOAT"
    assert w._map_polars_type_to_sql(pl.Float64) == "DOUBLE"
    assert w._map_polars_type_to_sql(pl.Boolean) == "BOOLEAN"
    assert w._map_polars_type_to_sql(pl.Date) == "DATE"
    tz_dtype = pl.Datetime(time_zone="UTC")
    assert w._map_polars_type_to_sql(tz_dtype) == "TIMESTAMPTZ"
    naive_dt = pl.Datetime
    assert w._map_polars_type_to_sql(naive_dt) == "TIMESTAMP"
    assert w._map_polars_type_to_sql(pl.Duration) == "INTERVAL"
    assert w._map_polars_type_to_sql(pl.String) == "VARCHAR"
    assert w._map_polars_type_to_sql(pl.Categorical) == "VARCHAR"
    # Complex types map to VARCHAR
    assert w._map_polars_type_to_sql(pl.Struct({"a": pl.Int32})) == "VARCHAR"
    assert w._map_polars_type_to_sql(pl.List(pl.Int32)) == "VARCHAR"


def test_identifier_validation_and_quoting() -> None:
    w = DuckDBWriter(":memory:")
    assert w._quote_identifier("valid_name_123") == '"valid_name_123"'
    with pytest.raises(InputError):
        w._quote_identifier("bad-name")  # '-' not allowed
    with pytest.raises(InputError):
        w._quote_identifier("bad\x07name")  # control char


@pytest.mark.asyncio
async def test_ensure_table_exists_missing_pk_skips_index(tmp_path: Path) -> None:
    db_path = tmp_path / "no_pk_index.duckdb"
    w = DuckDBWriter(db_path)
    con = w._get_connection()
    # Table with columns missing part of PK for yfinance_price (missing 'date')
    df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["ZZZ"], "close": [1.0]})
    w._ensure_table_exists(con, "yfinance_price", df)
    # Should have created table without unique index due to missing PK column(s)
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


@pytest.mark.asyncio
async def test_write_pk_null_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "pk_null.duckdb"
    w = DuckDBWriter(db_path)
    # Null 'date' PK column
    df = pl.DataFrame(
        {
            "provider": ["yfinance"],
            "ticker": ["AAPL"],
            "date": [None],
            "close": [1.0],
        }
    ).with_columns(pl.col("date").cast(pl.Date))
    pkt = FramePacket(
        provider="yfinance",
        table="yfinance_price",
        frame=df,
        observed_at=datetime.now(timezone.utc),
    )
    with pytest.raises(PrimaryKeyNullError):
        await w.write(pkt)
