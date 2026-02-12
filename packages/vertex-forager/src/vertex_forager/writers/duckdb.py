from __future__ import annotations

import asyncio
import logging
import time
import functools
from pathlib import Path
from typing import Sequence

import duckdb
import polars as pl

from vertex_forager.core.config import FramePacket
from vertex_forager.writers.base import BaseWriter, WriteResult


class DuckDBWriter(BaseWriter):
    """
    DuckDB Writer implementation using asyncio for concurrency control.
    
    Features:
    - Single-writer enforcement via asyncio.Lock
    - Automatic table creation from Polars schema
    - Upsert support (INSERT OR REPLACE)
    - Zero-copy data transfer from Polars to DuckDB
    """

    def __init__(self, db_path: str | Path) -> None:
        """
        Initialize DuckDB writer.

        Args:
            db_path: Path to the DuckDB database file.
        """
        self.db_path = str(db_path)
        self._lock = asyncio.Lock()
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._logger = logging.getLogger(__name__)
        # Cache for table existence and schema (table_name -> set of column names)
        self._table_schemas: dict[str, set[str]] = {}
        
    def _quote_identifier(self, identifier: str) -> str:
        """Safely quote an identifier (table or column name) for DuckDB."""
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a DuckDB connection."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            # Enable automatic checkpointing for safety
            # OPTIMIZATION: Increased WAL limit to 1GB to reduce checkpointing overhead during burst writes
            try:
                self._conn.execute("PRAGMA wal_autocheckpoint='1GB'") 
            except Exception as e:
                self._logger.warning(f"Failed to set wal_autocheckpoint: {e}")
        return self._conn

    async def write(self, packet: FramePacket) -> WriteResult:
        """
        Write a single packet to DuckDB immediately.
        Stateless implementation (no buffering).
        """
        if packet.frame.is_empty():
            return WriteResult(table=packet.table, rows=0)

        results = await self._write_internal([packet])
        return results[0] if results else WriteResult(table=packet.table, rows=0)

    async def write_bulk(self, packets: list[FramePacket]) -> list[WriteResult]:
        """
        Write a bulk of packets to DuckDB immediately.
        Stateless implementation (no buffering).
        """
        if not packets:
            return []
            
        # Group by table
        return await self._write_internal(packets)

    async def _write_internal(self, packets: Sequence[FramePacket]) -> list[WriteResult]:
        """
        Write a bulk of packets to DuckDB.
        Uses a single connection lock to ensure thread safety (DuckDB limitation).
        OFFLOADED: Runs in a thread pool to avoid blocking the asyncio event loop.
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, functools.partial(self._write_sync, packets))

    def _write_sync(self, packets: Sequence[FramePacket]) -> list[WriteResult]:
        """Synchronous implementation of write logic for offloading."""
        # Assuming all packets in the bulk might belong to different tables
        # But for optimization, we group them.
        
        # Log start
        total_rows = sum(len(p.frame) for p in packets)
        self._logger.debug(f"DUCKDB: Starting sync write for {len(packets)} packets ({total_rows} rows)")
        t0 = time.monotonic()
        
        by_table: dict[str, list[pl.DataFrame]] = {}
        results: list[WriteResult] = []
        
        for p in packets:
            if p.frame.is_empty():
                continue
            if p.table not in by_table:
                by_table[p.table] = []
            by_table[p.table].append(p.frame)

        
        if not by_table:
            return []

        # In sync mode, we assume exclusive access provided by caller (async lock)
        # or single-threaded execution context.
        conn = self._get_connection()
        
        for table_name, dfs in by_table.items():
            try:
                merged_df = pl.concat(dfs)
                rows = len(merged_df)
                if rows == 0:
                    continue
                    
                self._ensure_table_exists(conn, table_name, merged_df)
                rows_affected = self._upsert_data(conn, table_name, merged_df)
                
                results.append(WriteResult(table=table_name, rows=rows_affected))
                
            except Exception as e:
                self._logger.error(f"Failed to write batch for {table_name}: {e}")
                raise e
        
        t1 = time.monotonic()
        self._logger.debug(f"DUCKDB: Finished sync write in {t1 - t0:.3f}s. Results: {len(results)}")
        return results

    def _flush_table(self, table_name: str) -> WriteResult:
        """Deprecated: Internal buffer removed."""
        return WriteResult(table=table_name, rows=0)

    async def flush(self) -> None:
        """Flush is now no-op as we write immediately."""
        pass


    def compact(self) -> None:
        """
        Optimize database storage.
        Runs VACUUM and CHECKPOINT to reclaim space and enforce compression.
        """
        if self._conn:
            self._logger.info("Compacting DuckDB database...")
            self._conn.execute("VACUUM")
            self._conn.execute("CHECKPOINT")
            self._logger.info("Compaction completed.")

    async def close(self) -> None:
        """Close the database connection.
        
        Acquires the lock to ensure no write operations are in progress
        before closing the connection.
        """
        # Flush any remaining data first (outside lock to avoid deadlock)
        await self.flush()

        async with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception as e:
                    self._logger.warning(f"Error closing DuckDB connection: {e}")
                finally:
                    self._conn = None

    def _get_primary_keys(self, table_name: str) -> tuple[str, ...]:
        """
        Retrieve primary keys from the central schema registry.
        
        This replaces the hardcoded KNOWN_PRIMARY_KEYS dictionary.
        Now the schema definition controls the deduplication logic.
        """
        from vertex_forager.schema.registry import get_table_schema
        
        schema = get_table_schema(table_name)
        if schema and schema.unique_key:
            return schema.unique_key
        return ()

    def _ensure_table_exists(
        self, 
        conn: duckdb.DuckDBPyConnection, 
        table_name: str, 
        df: pl.DataFrame
    ) -> None:
        """
        Ensure table exists and has all required columns.
        Handles both table creation and schema evolution (adding new columns).
        """
        
        # OPTIMIZATION: Check memory cache first
        if table_name in self._table_schemas:
            existing_cols = self._table_schemas[table_name]
            # Fast path: if all new columns are already known, return
            if all(col in existing_cols for col in df.columns):
                return
            
            # Slow path: Add missing columns
            self._sync_schema(conn, table_name, df)
            return

        # Check if table exists in DB
        exists = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name]
        ).fetchone()[0] > 0

        if not exists:
            self._logger.info(f"Creating table '{table_name}' in DuckDB")
            # Register the dataframe as a view to infer schema
            conn.register('temp_df_view', df)
            
            # Create the table structure
            q_table = self._quote_identifier(table_name)
            conn.execute(f'CREATE TABLE {q_table} AS SELECT * FROM temp_df_view WHERE 1=0')
            
            # Apply Primary Key constraint from Schema Registry
            pk_cols = self._get_primary_keys(table_name)
            
            if pk_cols:
                # Check if all PK columns exist in the dataframe
                if all(col in df.columns for col in pk_cols):
                    pk_str = ", ".join(self._quote_identifier(c) for c in pk_cols)
                    try:
                        conn.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_pk" ON {q_table} ({pk_str})')
                        self._logger.info(f"Created UNIQUE INDEX on {table_name}({pk_str})")
                    except Exception as e:
                        self._logger.warning(f"Failed to create unique index on {table_name}: {e}")
                else:
                    missing = [col for col in pk_cols if col not in df.columns]
                    self._logger.warning(f"Skipping UNIQUE INDEX for {table_name}: Missing PK columns {missing} in dataframe")
            
            conn.unregister('temp_df_view')
            
            # Update Cache
            self._table_schemas[table_name] = set(df.columns)
        else:
            # Table exists but not in cache yet (first run or restart)
            # Fetch existing schema
            q_table = self._quote_identifier(table_name)
            existing_info = conn.execute(f'DESCRIBE {q_table}').fetchall()
            existing_cols = {row[0] for row in existing_info}
            self._table_schemas[table_name] = existing_cols
            
            # Schema Evolution: Add missing columns
            self._sync_schema(conn, table_name, df)

    def _sync_schema(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        df: pl.DataFrame
    ) -> None:
        """
        Check for missing columns in the table and add them if necessary.
        """
        # Get existing columns from cache if available, else DB
        if table_name in self._table_schemas:
            existing_cols = self._table_schemas[table_name]
        else:
            q_table = self._quote_identifier(table_name)
            existing_info = conn.execute(f'DESCRIBE {q_table}').fetchall()
            existing_cols = {row[0] for row in existing_info}
            self._table_schemas[table_name] = existing_cols

        # Find new columns
        new_cols = [col for col in df.columns if col not in existing_cols]
        
        if not new_cols:
            return

        self._logger.info(f"Schema evolution: Adding {len(new_cols)} new columns to {table_name}")
        
        q_table = self._quote_identifier(table_name)
        # Add each new column
        for col_name in new_cols:
            dtype = df.schema[col_name]
            sql_type = self._map_polars_type_to_sql(dtype)
            q_col = self._quote_identifier(col_name)
            
            try:
                self._logger.info(f"Adding column {col_name} ({sql_type}) to {table_name}")
                conn.execute(f'ALTER TABLE {q_table} ADD COLUMN {q_col} {sql_type}')
                # Update Cache
                self._table_schemas[table_name].add(col_name)
            except Exception as e:
                self._logger.error(f"Failed to add column {col_name} to {table_name}: {e}")
                # We continue to try adding other columns, though this might cause insert failure later

    def _map_polars_type_to_sql(self, dtype: pl.DataType) -> str:
        """Map Polars DataType to DuckDB SQL type string."""
        # Basic mapping
        if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            return "BIGINT"
        elif dtype in (pl.Float32, pl.Float64):
            return "DOUBLE"
        elif dtype == pl.Boolean:
            return "BOOLEAN"
        elif dtype == pl.Date:
            return "DATE"
        elif dtype in (pl.Datetime, pl.Duration):
            return "TIMESTAMP"
        elif dtype in (pl.Utf8, pl.String, pl.Categorical):
            return "VARCHAR"
        elif isinstance(dtype, pl.Struct):
            # Complex types fallback to VARCHAR (JSON string or similar often safer if structure varies)
            # Or try to rely on DuckDB's struct support, but string is safest for now
            return "VARCHAR" 
        elif isinstance(dtype, pl.List):
            # List types
            return "VARCHAR" # Simplified handling
        else:
            return "VARCHAR" # Default fallback

    def _upsert_data(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        df: pl.DataFrame
    ) -> int:
        """
        Insert data into the table, handling conflicts if unique index exists.
        """
        
        # Determine Primary Key
        pk_cols = self._get_primary_keys(table_name)
        
        q_table = self._quote_identifier(table_name)
        
        # If no PK, simple append
        if not pk_cols:
            conn.register('temp_batch_view', df)
            # Use explicit column names to handle schema evolution (new columns in table but not in this batch)
            cols_str = ", ".join(self._quote_identifier(c) for c in df.columns)
            conn.execute(f'INSERT INTO {q_table} ({cols_str}) SELECT {cols_str} FROM temp_batch_view')
            conn.unregister('temp_batch_view')
            return len(df)
            
        # If PK exists, perform Upsert
        # DuckDB's INSERT OR REPLACE / ON CONFLICT logic
        conn.register('temp_batch_view', df)
        
        pk_str = ", ".join(self._quote_identifier(c) for c in pk_cols)
        cols = df.columns
        cols_str = ", ".join(self._quote_identifier(c) for c in cols)
        
        # Using INSERT OR IGNORE or INSERT OR REPLACE
        # For financial data, we typically want REPLACE to update corrections
        # Explicitly list columns to ensure order independence and handle missing columns in batch
        
        # If there are no columns to update (only PK columns), we do NOTHING
        if all(col in pk_cols for col in cols):
            query = f"""
            INSERT INTO {q_table} ({cols_str})
            SELECT {cols_str} FROM temp_batch_view
            ON CONFLICT ({pk_str}) DO NOTHING
            """
        else:
            update_set = ", ".join([f'{self._quote_identifier(col)} = EXCLUDED.{self._quote_identifier(col)}' for col in cols if col not in pk_cols])
            query = f"""
            INSERT INTO {q_table} ({cols_str})
            SELECT {cols_str} FROM temp_batch_view
            ON CONFLICT ({pk_str}) DO UPDATE SET {update_set}
            """
            
        conn.execute(query)
        conn.unregister('temp_batch_view')
        
        return len(df)
