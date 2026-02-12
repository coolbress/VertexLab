import asyncio
from datetime import datetime

import duckdb
import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.writers import create_writer


class TestDuckDBWriter:
    """Test suite for DuckDBWriter."""

    @pytest.mark.asyncio
    async def test_writer_initialization_and_creation(self, tmp_path):
        """Test that create_writer returns a DuckDBWriter for duckdb:// scheme."""
        db_path = tmp_path / "test.duckdb"
        uri = f"duckdb://{db_path}"
        
        writer = create_writer(uri)
        assert isinstance(writer, DuckDBWriter)
        assert writer.db_path == str(db_path)

    @pytest.mark.asyncio
    async def test_write_single_packet(self, tmp_path):
        """Test writing a single data packet to DuckDB."""
        db_path = tmp_path / "test.duckdb"
        async with DuckDBWriter(db_path) as writer:
            df = pl.DataFrame({
                "ticker": ["AAPL", "MSFT"],
                "price": [150.0, 300.0],
                "date": ["2024-01-01", "2024-01-01"]
            })
            
            packet = FramePacket(
                provider="test_provider",
                table="prices",
                frame=df,
                observed_at=datetime.now()
            )
            
            result = await writer.write(packet)
        
        assert result.rows == 2
        assert result.table == "prices"
        
        # Verify data in DuckDB
        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT count(*) FROM prices").fetchone()[0]
        assert count == 2
        conn.close()

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, tmp_path):
        """Test concurrent writes to ensure locking works correctly."""
        db_path = tmp_path / "concurrent.duckdb"
        async with DuckDBWriter(db_path) as writer:
            # Create 100 packets with 10 rows each
            packets = []
            for i in range(100):
                df = pl.DataFrame({
                    "id": range(i * 10, (i + 1) * 10),
                    "val": [i] * 10
                })
                packets.append(FramePacket(
                    provider="test",
                    table="concurrent_test",
                    frame=df,
                    observed_at=datetime.now()
                ))
            
            # Run writes concurrently
            # Although DuckDBWriter uses a lock, asyncio.gather will schedule them
            # and we want to ensure no "database locked" errors occur.
            await asyncio.gather(*(writer.write(p) for p in packets))
        
        # Verify total rows
        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT count(*) FROM concurrent_test").fetchone()[0]
        assert count == 1000  # 100 packets * 10 rows
        conn.close()

    @pytest.mark.asyncio
    async def test_upsert_behavior(self, tmp_path):
        """Test that data is UPSERTED (deduplicated) when PK is known."""
        db_path = tmp_path / "upsert.duckdb"
        async with DuckDBWriter(db_path) as writer:
            # Use "sharadar_sep" table which has known PK in schema registry: [provider, ticker, date]
            # Note: Schema definition requires provider column for PK
            df1 = pl.DataFrame({
                "provider": ["sharadar"],
                "ticker": ["AAPL"], 
                "date": [datetime(2024, 1, 1).date()],
                "close": [100.0]
            })
            await writer.write(FramePacket(
                    provider="sharadar", 
                    table="sharadar_sep", 
                    frame=df1,
                    observed_at=datetime.now()
                ))
            
            # Insert same PK with different value
            df2 = pl.DataFrame({
                "provider": ["sharadar"],
                "ticker": ["AAPL"], 
                "date": [datetime(2024, 1, 1).date()],
                "close": [200.0]
            })
            await writer.write(FramePacket(
                provider="sharadar", 
                table="sharadar_sep", 
                frame=df2,
                observed_at=datetime.now()
            ))
        
        conn = duckdb.connect(str(db_path))
        # Should be 1 row with updated price
        rows = conn.execute("SELECT * FROM sharadar_sep").fetchall()
        assert len(rows) == 1
        # Check close price
        res = conn.execute("SELECT close FROM sharadar_sep").fetchone()
        assert res[0] == 200.0
        conn.close()

    @pytest.mark.asyncio
    async def test_write_bulk_small_data(self, tmp_path):
        """Test writing a small bulk (less than limit) works immediately."""
        db_path = tmp_path / "small_batch.duckdb"
        writer = DuckDBWriter(db_path)
        
        # Create just 2 packets (far less than 10,000 rows)
        packets = []
        for i in range(2):
            df = pl.DataFrame({"id": [i], "val": [i*10]})
            packets.append(FramePacket(
                provider="test",
                table="small_test",
                frame=df,
                observed_at=datetime.now()
            ))
        
        # Write bulk immediately
        results = await writer.write_bulk(packets)
        
        # Since both packets are for the same table, they are merged into one write operation
        assert len(results) == 1
        assert results[0].rows == 2

        
        # Verify data in DuckDB
        conn = duckdb.connect(str(db_path))
        count = conn.execute("SELECT count(*) FROM small_test").fetchone()[0]
        assert count == 2
        conn.close()
