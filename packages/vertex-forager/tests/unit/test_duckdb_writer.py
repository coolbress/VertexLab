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
            # "sharadar_sep" has known PK: [provider, ticker, date]
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
                    table="sharadar_sep",
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
                    table="sharadar_sep",
                    frame=df2,
                    observed_at=datetime.now(),
                )
            )

        conn = duckdb.connect(str(db_path))
        # Should be 1 row with updated price
        rows = conn.execute("SELECT * FROM sharadar_sep").fetchall()
        assert rows is not None
        assert len(rows) == 1
        # Check close price
        res = conn.execute("SELECT close FROM sharadar_sep").fetchone()
        assert res is not None
        assert res[0] == 200.0
        conn.close()

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
