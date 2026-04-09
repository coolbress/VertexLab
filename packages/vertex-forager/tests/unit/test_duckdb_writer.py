import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.writers import create_writer
from vertex_forager.writers.duckdb import DuckDBWriter


class TestDuckDBWriter:
    """Test suite for DuckDBWriter."""

    @pytest.mark.asyncio
    async def test_writer_initialization_and_creation(self, tmp_path: Path) -> None:
        """create_writer returns a DuckDBWriter for duckdb:// scheme."""
        db_path = tmp_path / "test.duckdb"
        uri = f"duckdb://{db_path}"

        writer = create_writer(uri)
        assert isinstance(writer, DuckDBWriter)
        assert writer.db_path == str(db_path)

    @pytest.mark.asyncio
    async def test_write_single_packet(self, tmp_path: Path) -> None:
        """Test writing a single data packet to DuckDB."""
        db_path = tmp_path / "test.duckdb"
        async with DuckDBWriter(db_path) as writer:
            df = pl.DataFrame(
                {
                    "ticker": ["AAPL", "MSFT"],
                    "price": [150.0, 300.0],
                    "date": ["2024-01-01", "2024-01-01"],
                }
            )

            packet = FramePacket(
                provider="test_provider",
                table="prices",
                frame=df,
                observed_at=datetime.now(),
            )

            result = await writer.write(packet)

        assert result.rows == 2
        assert result.table == "prices"

        # Verify data in DuckDB
        conn = duckdb.connect(str(db_path))
        row = conn.execute("SELECT count(*) FROM prices").fetchone()
        assert row is not None
        count = row[0]
        assert count == 2
        conn.close()

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, tmp_path: Path) -> None:
        """Concurrent writes ensure locking works correctly."""
        db_path = tmp_path / "concurrent.duckdb"
        async with DuckDBWriter(db_path) as writer:
            # Create 100 packets with 10 rows each
            packets = [
                FramePacket(
                    provider="test",
                    table="concurrent_test",
                    frame=pl.DataFrame(
                        {
                            "id": range(i * 10, (i + 1) * 10),
                            "val": [i] * 10,
                        }
                    ),
                    observed_at=datetime.now(),
                )
                for i in range(100)
            ]

            # Run writes concurrently
            # DuckDBWriter uses a lock; asyncio.gather schedules them
            # Ensure no "database locked" errors occur.
            await asyncio.gather(*(writer.write(p) for p in packets))

        # Verify total rows
        conn = duckdb.connect(str(db_path))
        row = conn.execute("SELECT count(*) FROM concurrent_test").fetchone()
        assert row is not None
        count = row[0]
        assert count == 1000  # 100 packets * 10 rows
        conn.close()

    @pytest.mark.asyncio
    async def test_upsert_behavior(self, tmp_path: Path) -> None:
        """Test that data is UPSERTED (deduplicated) when PK is known."""
        db_path = tmp_path / "upsert.duckdb"
        async with DuckDBWriter(db_path) as writer:
            # "sharadar_price" has known PK: [provider, ticker, date]
            # Note: provider column required for PK
            df1 = pl.DataFrame(
                {
                    "provider": ["sharadar"],
                    "ticker": ["AAPL"],
                    "date": [datetime(2024, 1, 1).date()],
                    "close": [100.0],
                }
            )
            await writer.write(
                FramePacket(
                    provider="sharadar",
                    table="sharadar_price",
                    frame=df1,
                    observed_at=datetime.now(),
                )
            )

            # Insert same PK with different value
            df2 = pl.DataFrame(
                {
                    "provider": ["sharadar"],
                    "ticker": ["AAPL"],
                    "date": [datetime(2024, 1, 1).date()],
                    "close": [200.0],
                }
            )
            await writer.write(
                FramePacket(
                    provider="sharadar",
                    table="sharadar_price",
                    frame=df2,
                    observed_at=datetime.now(),
                )
            )

        conn = duckdb.connect(str(db_path))
        # Should be 1 row with updated price
        rows = conn.execute("SELECT * FROM sharadar_price").fetchall()
        assert rows is not None
        assert len(rows) == 1
        # Check close price
        res = conn.execute("SELECT close FROM sharadar_price").fetchone()
        assert res is not None
        assert res[0] == 200.0
        conn.close()

    @pytest.mark.asyncio
    async def test_shared_table_discriminators_prevent_overwrite(self, tmp_path: Path) -> None:
        db_path = tmp_path / "discriminators.duckdb"
        async with DuckDBWriter(db_path) as writer:
            financials = pl.DataFrame(
                {
                    "provider": ["yfinance"],
                    "ticker": ["AAPL"],
                    "date": [datetime(2024, 1, 1).date()],
                    "period": ["annual"],
                    "statement_kind": ["income_stmt"],
                    "metric": ["EBITDA"],
                    "value": [100.0],
                }
            )
            balance_sheet = financials.with_columns(
                pl.lit("balance_sheet").alias("statement_kind"),
                pl.lit(200.0).alias("value"),
            )
            holders = pl.DataFrame(
                {
                    "provider": ["yfinance"],
                    "ticker": ["AAPL"],
                    "holder_type": ["institutional"],
                    "holder": ["Fund A"],
                    "date_reported": [datetime(2024, 1, 1).date()],
                    "shares": [100.0],
                    "pctheld": [0.1],
                    "pctchange": [0.0],
                    "value": [1000.0],
                }
            )
            mutualfund = holders.with_columns(
                pl.lit("mutualfund").alias("holder_type"),
                pl.lit(200.0).alias("shares"),
            )
            await writer.write(
                FramePacket(
                    provider="yfinance",
                    table="yfinance_financials",
                    frame=financials,
                    observed_at=datetime.now(),
                )
            )
            await writer.write(
                FramePacket(
                    provider="yfinance",
                    table="yfinance_financials",
                    frame=balance_sheet,
                    observed_at=datetime.now(),
                )
            )
            await writer.write(
                FramePacket(
                    provider="yfinance",
                    table="yfinance_holders",
                    frame=holders,
                    observed_at=datetime.now(),
                )
            )
            await writer.write(
                FramePacket(
                    provider="yfinance",
                    table="yfinance_holders",
                    frame=mutualfund,
                    observed_at=datetime.now(),
                )
            )

        with duckdb.connect(str(db_path)) as conn:
            financial_count = conn.execute("SELECT count(*) FROM yfinance_financials").fetchone()
            holders_count = conn.execute("SELECT count(*) FROM yfinance_holders").fetchone()
            assert financial_count is not None
            assert holders_count is not None
            assert financial_count[0] == 2
            assert holders_count[0] == 2

    @pytest.mark.asyncio
    async def test_write_bulk_small_data(self, tmp_path: Path) -> None:
        """Test writing a small bulk (less than limit) works immediately."""
        db_path = tmp_path / "small_batch.duckdb"
        async with DuckDBWriter(db_path) as writer:
            # Create just 2 packets (far less than 10,000 rows)
            packets = [
                FramePacket(
                    provider="test",
                    table="small_test",
                    frame=pl.DataFrame({"id": [i], "val": [i * 10]}),
                    observed_at=datetime.now(),
                )
                for i in range(2)
            ]

            # Write bulk immediately
            results = await writer.write_bulk(packets)

            # Now returns 1:1 results for each packet
            assert len(results) == 2
            assert results[0].rows == 1
            assert results[1].rows == 1

        # Verify data in DuckDB
        conn = duckdb.connect(str(db_path))
        row = conn.execute("SELECT count(*) FROM small_test").fetchone()
        assert row is not None
        count = row[0]
        assert count == 2
        conn.close()

    @pytest.mark.asyncio
    async def test_write_bulk_reports_post_upsert_rows(self, tmp_path: Path) -> None:
        db_path = tmp_path / "bulk_upsert.duckdb"
        async with DuckDBWriter(db_path) as writer:
            packets = [
                FramePacket(
                    provider="yfinance",
                    table="yfinance_price",
                    frame=pl.DataFrame(
                        {
                            "provider": ["yfinance"],
                            "ticker": ["AAPL"],
                            "date": [datetime(2024, 1, 1).date()],
                            "close": [100.0],
                        }
                    ),
                    observed_at=datetime.now(),
                ),
                FramePacket(
                    provider="yfinance",
                    table="yfinance_price",
                    frame=pl.DataFrame(
                        {
                            "provider": ["yfinance"],
                            "ticker": ["AAPL"],
                            "date": [datetime(2024, 1, 1).date()],
                            "close": [200.0],
                        }
                    ),
                    observed_at=datetime.now(),
                ),
            ]

            results = await writer.write_bulk(packets)

        assert [res.rows for res in results] == [0, 1]
        with duckdb.connect(str(db_path)) as conn:
            row = conn.execute("SELECT count(*) FROM yfinance_price").fetchone()
            assert row is not None
            assert row[0] == 1
            close = conn.execute("SELECT close FROM yfinance_price").fetchone()
            assert close is not None
            assert close[0] == 200.0

    @pytest.mark.asyncio
    async def test_duckdb_writer_resets_connection_after_write_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _FakeConn:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        writer = DuckDBWriter(tmp_path / "error_reset.duckdb")
        fake_conn = _FakeConn()
        writer._conn = cast(duckdb.DuckDBPyConnection, fake_conn)

        def _raise_write_error(**_: Any) -> None:
            raise duckdb.Error("boom")

        monkeypatch.setattr(writer, "_write_table_entries", _raise_write_error)

        packet = FramePacket(
            provider="test",
            table="prices",
            frame=pl.DataFrame({"ticker": ["AAPL"], "price": [150.0], "date": ["2024-01-01"]}),
            observed_at=datetime.now(),
        )

        with pytest.raises(duckdb.Error, match="boom"):
            await writer.write(packet)

        assert writer._conn is None
        assert fake_conn.closed is True
        await writer.close()

    @pytest.mark.asyncio
    async def test_unsigned_integer_columns_map_to_duckdb_unsigned_types(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "uint_types.duckdb"
        async with DuckDBWriter(db_path) as writer:
            df = pl.DataFrame(
                {
                    "provider": ["test"],
                    "ticker": ["UINT"],
                    "date": [datetime(2024, 1, 1).date()],
                    "u8": pl.Series([1], dtype=pl.UInt8),
                    "u16": pl.Series([2], dtype=pl.UInt16),
                    "u32": pl.Series([3], dtype=pl.UInt32),
                    "u64": pl.Series([4], dtype=pl.UInt64),
                }
            )
            await writer.write(
                FramePacket(
                    provider="test",
                    table="unsigned_types",
                    frame=df,
                    observed_at=datetime.now(),
                )
            )
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute('DESCRIBE "unsigned_types"').fetchall()
            type_map = {r[0]: str(r[1]).upper() for r in rows}
            assert type_map["u8"] == "UTINYINT"
            assert type_map["u16"] == "USMALLINT"
            assert type_map["u32"] == "UINTEGER"
            assert type_map["u64"] == "UBIGINT"


def test_compact_sync_checkpoint_warning_on_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    class _FakeConn:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, sql: str) -> Any:
            self.calls += 1
            if self.calls == 2:
                raise duckdb.Error("unsupported")
            return None

    caplog.set_level(logging.WARNING)
    writer = DuckDBWriter(tmp_path / "t.duckdb")
    fake = _FakeConn()
    writer._conn = cast(duckdb.DuckDBPyConnection, fake)
    writer._compact_sync()
    assert fake.calls == 2
    assert any(
        "CHECKPOINT failed or unsupported" in rec.message for rec in caplog.records
    )


def test_compact_sync_checkpoint_ok(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    class _FakeConnOK:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, sql: str) -> Any:
            self.calls += 1
            return None

    writer = DuckDBWriter(tmp_path / "t2.duckdb")
    fake = _FakeConnOK()
    writer._conn = cast(duckdb.DuckDBPyConnection, fake)
    caplog.set_level(logging.WARNING)
    writer._compact_sync()
    assert fake.calls == 2
    assert not any(
        "CHECKPOINT failed or unsupported" in rec.message for rec in caplog.records
    )
