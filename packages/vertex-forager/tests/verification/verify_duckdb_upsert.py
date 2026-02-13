import asyncio
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
    async with DuckDBWriter(db_path) as writer:
        # Helper to print table content
        def print_table(msg: str):
            conn = duckdb.connect(str(db_path))
            try:
                df = conn.execute(
                    "SELECT provider, ticker, date, close FROM sharadar_sep ORDER BY date"
                ).pl()
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
        df1 = pl.DataFrame(
            {
                "provider": ["sharadar"],
                "ticker": ["AAPL"],
                "date": [date(2024, 1, 1)],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [100.0],
                "volume": [1000],
                "closeadj": [100.0],
                "closeunadj": [100.0],
                "lastupdated": [date(2024, 1, 1)],
                "fetched_at": [datetime.now()],
            }
        )

        packet1 = FramePacket(
            provider="sharadar",
            table="sharadar_sep",
            frame=df1,
            observed_at=datetime.now(),
        )

        await writer.write(packet1)
        print_table("After Step 1")

        # Step 2: Update Data (Same PK, Different Close Price)
        print("\n--- 3. Insert Update Data (AAPL, 2024-01-01, Close=150.0) ---")
        df2 = pl.DataFrame(
            {
                "provider": ["sharadar"],
                "ticker": ["AAPL"],
                "date": [date(2024, 1, 1)],
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [150.0],
                "volume": [1000],
                "closeadj": [150.0],
                "closeunadj": [150.0],
                "lastupdated": [date(2024, 1, 1)],
                "fetched_at": [datetime.now()],
            }
        )

        packet2 = FramePacket(
            provider="sharadar",
            table="sharadar_sep",
            frame=df2,
            observed_at=datetime.now(),
        )

        await writer.write(packet2)
        print_table("After Step 2")

    # Verification
    print("\n--- 4. Verification ---")
    conn = duckdb.connect(str(db_path))
    result = conn.execute(
        "SELECT close FROM sharadar_sep WHERE ticker='AAPL' AND date='2024-01-01'"
    ).fetchone()
    conn.close()

    if result and result[0] == 150.0:
        print("✅ SUCCESS: Data was upserted correctly (Close price updated to 150.0).")
    else:
        print(f"❌ FAILURE: Expected Close=150.0, got {result}")

    # Cleanup
    if db_path.exists():
        db_path.unlink()


if __name__ == "__main__":
    asyncio.run(main())
