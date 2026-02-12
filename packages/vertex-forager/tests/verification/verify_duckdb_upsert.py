
import asyncio
import shutil
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl
from vertex_forager.core.config import FramePacket
from vertex_forager.writers.duckdb import DuckDBWriter

async def main():
    # Setup
    db_path = Path("test_upsert.duckdb")
    if db_path.exists():
        db_path.unlink()
    
    print(f"--- 1. Initialize DuckDB Writer at {db_path} ---")
    writer = DuckDBWriter(db_path)
    
    # Helper to print table content
    def print_table(msg: str):
        conn = duckdb.connect(str(db_path))
        try:
            df = conn.execute("SELECT provider, ticker, date, close FROM sharadar_sep ORDER BY date").pl()
            print(f"\n[{msg}] Table 'sharadar_sep':")
            if df.is_empty():
                print("(Empty)")
            else:
                print(df)
        except duckdb.CatalogException:
            print(f"\n[{msg}] Table 'sharadar_sep' does not exist yet.")
        finally:
            conn.close()

    print_table("Initial State")

    # Step 1: Insert Initial Data
    print("\n--- 2. Insert Initial Data (AAPL, 2024-01-01, Close=100.0) ---")
    df1 = pl.DataFrame({
        "provider": ["sharadar"],
        "ticker": ["AAPL"],
        "date": [date(2024, 1, 1)],
        "open": [100.0], "high": [105.0], "low": [99.0], "close": [100.0],
        "volume": [1000], "closeadj": [100.0], "closeunadj": [100.0],
        "lastupdated": [date(2024, 1, 1)],
        "fetched_at": [datetime.now()]
    })
    
    packet1 = FramePacket(
        provider="sharadar",
        table="sharadar_sep",
        frame=df1,
        observed_at=datetime.now()
    )
    
    await writer.write(packet1)
    print_table("After Step 1")

    # Step 2: Update Data (Upsert)
    print("\n--- 3. Update Data (AAPL, 2024-01-01, Close=105.0) ---")
    print("Key: (provider='sharadar', ticker='AAPL', date='2024-01-01') -> Same Key, New Value")
    
    df2 = pl.DataFrame({
        "provider": ["sharadar"],
        "ticker": ["AAPL"],
        "date": [date(2024, 1, 1)],
        "open": [100.0], "high": [105.0], "low": [99.0], "close": [105.0], # CHANGED
        "volume": [1000], "closeadj": [105.0], "closeunadj": [105.0],
        "lastupdated": [date(2024, 1, 2)], # CHANGED
        "fetched_at": [datetime.now()]
    })
    
    packet2 = FramePacket(
        provider="sharadar",
        table="sharadar_sep",
        frame=df2,
        observed_at=datetime.now()
    )
    
    await writer.write(packet2)
    print_table("After Step 2 (Should show Close=105.0)")

    # Step 3: Insert New Data
    print("\n--- 4. Insert New Data (AAPL, 2024-01-02, Close=110.0) ---")
    print("Key: (provider='sharadar', ticker='AAPL', date='2024-01-02') -> New Key")
    
    df3 = pl.DataFrame({
        "provider": ["sharadar"],
        "ticker": ["AAPL"],
        "date": [date(2024, 1, 2)], # DIFFERENT DATE
        "open": [105.0], "high": [110.0], "low": [104.0], "close": [110.0],
        "volume": [1200], "closeadj": [110.0], "closeunadj": [110.0],
        "lastupdated": [date(2024, 1, 2)],
        "fetched_at": [datetime.now()]
    })
    
    packet3 = FramePacket(
        provider="sharadar",
        table="sharadar_sep",
        frame=df3,
        observed_at=datetime.now()
    )
    
    await writer.write(packet3)
    print_table("After Step 3 (Should have 2 rows)")

    # Cleanup
    await writer.close()
    if db_path.exists():
        db_path.unlink()
    print("\n--- Done ---")

if __name__ == "__main__":
    asyncio.run(main())
