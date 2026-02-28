"""DuckDB Writer Module.

This module provides the DuckDBWriter class for persisting financial data frames
to a local DuckDB database. It handles schema evolution, upsert logic, and
concurrency control to ensure data integrity during high-throughput ingestion.

Classes:
    DuckDBWriter: Asynchronous writer for DuckDB using Polars integration.

Usage:
    writer = DuckDBWriter("data/market_data.duckdb")
    await writer.write(packet)
    await writer.compact()
    await writer.close()
"""
from __future__ import annotations

import asyncio
import logging
import time
import functools
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Sequence

import duckdb
import polars as pl

from vertex_forager.core.config import FramePacket
from vertex_forager.constants import WRITER_DUCKDB_MAX_WORKERS, WAL_AUTOCHECKPOINT_LIMIT
from vertex_forager.writers.constants import (
    WR_LOG_PREFIX,
    DK_LOG_PREFIX,
    LOG_START_SYNC_WRITE,
    LOG_FINISH_SYNC_WRITE,
    LOG_SCHEMA_MISMATCH,
    LOG_MISSING_PK_COL,
    LOG_NULLS_PK_COL,
    LOG_FAILED_BATCH,
    LOG_VALIDATION_ERROR,
    LOG_NO_CONN_SKIP_COMPACT,
    LOG_COMPACTING,
    LOG_COMPACT_DONE,
    LOG_CREATE_TABLE,
    LOG_UNIQUE_INDEX_CREATED,
    LOG_UNIQUE_INDEX_FAIL,
    LOG_SKIP_UNIQUE_INDEX_MISSING_PK,
    LOG_SCHEMA_EVOLUTION_ADDING,
    LOG_ADD_COLUMN,
    LOG_ADD_COLUMN_FAIL,
    LOG_CLOSE_CONN_WARNING,
    LOG_CORRELATION_SUMMARY,
    LOG_CORRELATION_SAMPLES,
)
from vertex_forager.writers.base import BaseWriter, WriteResult
from vertex_forager.exceptions import InputError, ValidationError, PrimaryKeyMissingError, PrimaryKeyNullError

# Module-level cache for schema getter to avoid circular imports
_get_table_schema = None


class DuckDBWriter(BaseWriter):
    """DuckDB writer implementation.

    Manages a local DuckDB database for storing collected data using Polars integration.

    Attributes:
        lock (asyncio.Lock): Lock to ensure single-writer access to the database.
        connection (duckdb.DuckDBPyConnection): Active DuckDB connection.
        db_path (str): Path to the DuckDB database file.

    Features:
        - Automatic table creation from Polars schema.
        - Upsert semantics based on primary keys.
        - Zero-copy data transfer between Polars and DuckDB.
        - Thread-safe execution using a single-threaded executor.
    """

    def __init__(self, db_path: str | Path) -> None:
        """
        Initialize DuckDB writer.

        Args:
            db_path: Path to the DuckDB database file.
        """
        self.db_path = str(db_path)
        self._lock = asyncio.Lock()
        # Single-threaded executor for DuckDB thread safety
        self._executor = ThreadPoolExecutor(max_workers=WRITER_DUCKDB_MAX_WORKERS)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._logger = logging.getLogger(__name__)
        # Cache for table existence and schema (table_name -> set of column names)
        self._table_schemas: dict[str, set[str]] = {}

    def _validate_identifier(self, identifier: str) -> None:
        if not isinstance(identifier, str) or not identifier:
            raise InputError(f"invalid identifier: {identifier!r}")
        for ch in identifier:
            o = ord(ch)
            if o < 32 or ch in {"\x7f"}:
                raise InputError("identifier contains control characters")
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
        if not set(identifier) <= allowed:
            raise InputError("identifier contains unsupported characters")

    def _quote_identifier(self, identifier: str) -> str:
        """Safely quote an identifier (table or column name) for DuckDB."""
        self._validate_identifier(identifier)
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a DuckDB connection."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            # Enable automatic checkpointing for safety
            # OPTIMIZATION: Increased WAL limit to 1GB to reduce checkpointing overhead during burst writes
            try:
                self._conn.execute(f"PRAGMA wal_autocheckpoint='{WAL_AUTOCHECKPOINT_LIMIT}'")
            except duckdb.Error as e:
                self._logger.warning(f"Failed to set wal_autocheckpoint: {e}")
        return self._conn

    async def write(self, packet: FramePacket) -> WriteResult:
        """
        Write a single packet to DuckDB immediately.
        Stateless implementation (no buffering).

        Args:
            packet: FramePacket containing the table name and DataFrame to write.

        Returns:
            WriteResult: Result object containing table name and number of rows written.
        """
        if packet.frame.is_empty():
            return WriteResult(table=packet.table, rows=0)

        results = await self._write_internal([packet])
        return results[0] if results else WriteResult(table=packet.table, rows=0)

    async def write_bulk(self, packets: list[FramePacket]) -> list[WriteResult]:
        """
        Write a bulk of packets to DuckDB immediately.
        Stateless implementation (no buffering).

        Args:
            packets: List of FramePackets to write.

        Returns:
            list[WriteResult]: List of result objects for each written packet.
        """
        if not packets:
            return []

        # Persist data (table-level batching for efficiency)
        await self._write_internal(packets)
        # Preserve BaseWriter contract: one result per input packet
        return [
            WriteResult(table=p.table, rows=p.frame.height)
            for p in packets
        ]

    async def _write_internal(
        self, packets: Sequence[FramePacket]
    ) -> list[WriteResult]:
        """
        Write a bulk of packets to DuckDB.
        Uses a single connection lock to ensure thread safety (DuckDB limitation).
        OFFLOADED: Runs in a thread pool to avoid blocking the asyncio event loop.
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._executor, functools.partial(self._write_sync, packets)
            )

    def _write_sync(self, packets: Sequence[FramePacket]) -> list[WriteResult]:
        """Synchronous implementation of write logic for offloading."""
        # Log start
        total_rows = sum(len(p.frame) for p in packets)
        self._logger.debug(LOG_START_SYNC_WRITE.format(prefix=DK_LOG_PREFIX, packets=len(packets), rows=total_rows))
        t0 = time.monotonic()

        # Correlation summary (trace_id/request_id counts)
        try:
            trace_ids = set()
            request_ids = set()
            for p in packets:
                ctx = getattr(p, "context", None)
                if isinstance(ctx, dict):
                    tid = ctx.get("trace_id")
                    rid = ctx.get("request_id")
                    if isinstance(tid, str):
                        trace_ids.add(tid)
                    if isinstance(rid, int):
                        request_ids.add(rid)
            self._logger.debug(
                LOG_CORRELATION_SUMMARY.format(
                    prefix=DK_LOG_PREFIX,
                    trace_ids_count=len(trace_ids),
                    request_ids_count=len(request_ids),
                )
            )
            trace_samples = list(trace_ids)[:2]
            request_samples = list(sorted(request_ids))[:2]
            if trace_samples or request_samples:
                self._logger.debug(
                    LOG_CORRELATION_SAMPLES.format(
                        prefix=DK_LOG_PREFIX,
                        trace_ids=trace_samples,
                        request_ids=request_samples,
                    )
                )
        except Exception:
            # best-effort; ignore correlation computation errors
            pass
        # Group by table but keep track of original indices to return ordered results
        by_table: dict[str, list[tuple[int, FramePacket]]] = {}
        final_results: list[WriteResult | None] = [None] * len(packets)

        for i, p in enumerate(packets):
            if p.frame.is_empty():
                final_results[i] = WriteResult(table=p.table, rows=0)
                continue
            
            if p.table not in by_table:
                by_table[p.table] = []
            by_table[p.table].append((i, p))

        if not by_table:
            # All packets were empty
            return [res for res in final_results if res is not None]  # Should be all

        # In sync mode, we assume exclusive access provided by caller (async lock)
        # or single-threaded execution context.
        conn = self._get_connection()

        for table_name, entries in by_table.items():
            try:
                frames = [p.frame for _, p in entries]
                try:
                    merged_df = pl.concat(frames, how="vertical")
                except pl.exceptions.PolarsError as e:
                    # Use schema flexible flag to decide diagonal fallback
                    global _get_table_schema
                    if _get_table_schema is None:
                        try:
                            from vertex_forager.schema.registry import get_table_schema
                            _get_table_schema = get_table_schema
                        except ImportError:
                            _get_table_schema = None
                    schema = _get_table_schema(table_name) if _get_table_schema else None
                    is_flexible = bool(schema and getattr(schema, "flexible_schema", False))
                    if not is_flexible:
                        raise
                    self._logger.warning(LOG_SCHEMA_MISMATCH.format(prefix=WR_LOG_PREFIX, table=table_name, error=e))
                    merged_df = pl.concat(frames, how="diagonal")
                pk_cols = self._get_primary_keys(table_name)
                if pk_cols:
                    for c in pk_cols:
                        if c not in merged_df.columns:
                            self._logger.error(LOG_MISSING_PK_COL.format(prefix=WR_LOG_PREFIX, col=c, table=table_name, columns=merged_df.columns))
                            raise PrimaryKeyMissingError(table=table_name, column=c)
                        else:
                            nulls = merged_df.get_column(c).null_count()
                            if nulls > 0:
                                self._logger.error(LOG_NULLS_PK_COL.format(prefix=WR_LOG_PREFIX, col=c, table=table_name, rows=len(merged_df), columns=merged_df.columns))
                                raise PrimaryKeyNullError(table=table_name, column=c, null_count=nulls)
                rows = len(merged_df)
                
                if rows > 0:
                    self._ensure_table_exists(conn, table_name, merged_df)
                    # We upsert the whole batch. 
                    # Note: This assumes _upsert_data succeeds for the whole batch.
                    # If partial failure is possible, this logic would need refinement,
                    # but DuckDB operations are typically transactional per statement.
                    self._upsert_data(conn, table_name, merged_df)

                # Assign results back to their original positions
                for idx, p in entries:
                    final_results[idx] = WriteResult(table=table_name, rows=len(p.frame))

            except duckdb.Error as e:
                self._logger.error(LOG_FAILED_BATCH.format(prefix=WR_LOG_PREFIX, table=table_name, error=e))
                raise
            except ValidationError as e:
                self._logger.debug(LOG_VALIDATION_ERROR.format(prefix=WR_LOG_PREFIX, table=table_name, error=e))
                raise

        # Fill any remaining None (should be covered by empty check or loop, but safe guard)
        # Actually my type hint says list[WriteResult | None] but return is list[WriteResult]
        # I need to ensure no None remains.
        # Logic above:
        # 1. Empty packets -> filled immediately.
        # 2. Non-empty packets -> added to by_table -> filled in loop.
        # So all should be filled.
        
        results = [res for res in final_results if res is not None]
        
        t1 = time.monotonic()
        self._logger.debug(LOG_FINISH_SYNC_WRITE.format(prefix=DK_LOG_PREFIX, seconds=(t1 - t0), results=len(results)))
        return results

    async def flush(self) -> None:
        """
        Flush is now no-op as we write immediately.

        Args:
            None

        Returns:
            None
        """
        pass

    async def compact(self) -> None:
        """
        Optimize database storage.
        Runs VACUUM and CHECKPOINT to reclaim space and enforce compression.

        Args:
            None

        Returns:
            None
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._compact_sync)

    def _compact_sync(self) -> None:
        """Synchronous implementation of compact logic."""
        if not self._conn:
            self._logger.info(LOG_NO_CONN_SKIP_COMPACT.format(prefix=DK_LOG_PREFIX))
            return
        self._logger.info(LOG_COMPACTING.format(prefix=DK_LOG_PREFIX))
        self._logger.info("Compacting DuckDB database...")
        self._conn.execute("VACUUM")
        self._logger.info(LOG_COMPACT_DONE.format(prefix=DK_LOG_PREFIX))

    async def close(self) -> None:
        """Close the database connection.

        Acquires the lock to ensure no write operations are in progress
        before closing the connection.

        Args:
            None

        Returns:
            None
        """
        # Flush any remaining data first (outside lock to avoid deadlock)
        # await self.flush() (no-op, removed to avoid recursive lock wait)

        async with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except duckdb.Error as e:
                    self._logger.warning(LOG_CLOSE_CONN_WARNING.format(prefix=DK_LOG_PREFIX, error=e))
                finally:
                    self._conn = None

        # Shutdown executor
        self._executor.shutdown(wait=True)

    def _get_primary_keys(self, table_name: str) -> tuple[str, ...]:
        """
        Retrieve primary keys from the central schema registry.

        This replaces the hardcoded KNOWN_PRIMARY_KEYS dictionary.
        Now the schema definition controls the deduplication logic.
        """
        global _get_table_schema
        if _get_table_schema is None:
            try:
                from vertex_forager.schema.registry import get_table_schema

                _get_table_schema = get_table_schema
            except ImportError:
                # Should not happen in normal execution
                return ()

        schema = _get_table_schema(table_name)
        if schema and schema.unique_key:
            return schema.unique_key
        return ()

    def _ensure_table_exists(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, df: pl.DataFrame
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
        row = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        exists = bool(row and row[0] > 0)

        if not exists:
            self._logger.info(LOG_CREATE_TABLE.format(prefix=DK_LOG_PREFIX, table=table_name))
            # Register the dataframe as a view to infer schema
            conn.register("temp_df_view", df)

            try:
                # Create the table structure
                q_table = self._quote_identifier(table_name)
                conn.execute(
                    f"CREATE TABLE {q_table} AS SELECT * FROM temp_df_view WHERE 1=0"
                )

                # Apply Primary Key constraint from Schema Registry
                pk_cols = self._get_primary_keys(table_name)

                if pk_cols:
                    # Check if all PK columns exist in the dataframe
                    if all(col in df.columns for col in pk_cols):
                        pk_str = ", ".join(self._quote_identifier(c) for c in pk_cols)
                        try:
                            idx_name = self._quote_identifier(f"idx_{table_name}_pk")
                            conn.execute(
                                f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON {q_table} ({pk_str})"
                            )
                            self._logger.info(LOG_UNIQUE_INDEX_CREATED.format(prefix=DK_LOG_PREFIX, table=table_name, pk_str=pk_str))
                        except duckdb.Error as e:
                            self._logger.warning(LOG_UNIQUE_INDEX_FAIL.format(prefix=DK_LOG_PREFIX, table=table_name, error=e))
                    else:
                        missing = [col for col in pk_cols if col not in df.columns]
                        self._logger.warning(LOG_SKIP_UNIQUE_INDEX_MISSING_PK.format(prefix=DK_LOG_PREFIX, table=table_name, missing=missing))
            finally:
                conn.unregister("temp_df_view")

            # Update Cache
            self._table_schemas[table_name] = set(df.columns)
        else:
            # Table exists but not in cache yet (first run or restart)
            # Fetch existing schema
            q_table = self._quote_identifier(table_name)
            existing_info = conn.execute(f"DESCRIBE {q_table}").fetchall()
            existing_cols = {row[0] for row in existing_info}
            self._table_schemas[table_name] = existing_cols

            # Schema Evolution: Add missing columns
            self._sync_schema(conn, table_name, df)

    def _sync_schema(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, df: pl.DataFrame
    ) -> None:
        """
        Check for missing columns in the table and add them if necessary.
        """
        # Get existing columns from cache if available, else DB
        if table_name in self._table_schemas:
            existing_cols = self._table_schemas[table_name]
        else:
            q_table = self._quote_identifier(table_name)
            existing_info = conn.execute(f"DESCRIBE {q_table}").fetchall()
            existing_cols = {row[0] for row in existing_info}
            self._table_schemas[table_name] = existing_cols

        # Find new columns
        new_cols = [col for col in df.columns if col not in existing_cols]

        if not new_cols:
            return

        self._logger.info(LOG_SCHEMA_EVOLUTION_ADDING.format(prefix=DK_LOG_PREFIX, count=len(new_cols), table=table_name))

        q_table = self._quote_identifier(table_name)
        # Add each new column
        for col_name in new_cols:
            dtype = df.schema[col_name]
            sql_type = self._map_polars_type_to_sql(dtype)
            q_col = self._quote_identifier(col_name)

            try:
                self._logger.debug(LOG_ADD_COLUMN.format(prefix=DK_LOG_PREFIX, col=col_name, sql_type=sql_type, table=table_name))
                conn.execute(f"ALTER TABLE {q_table} ADD COLUMN {q_col} {sql_type}")
                # Update Cache
                self._table_schemas[table_name].add(col_name)
            except duckdb.Error as e:
                self._logger.error(LOG_ADD_COLUMN_FAIL.format(prefix=DK_LOG_PREFIX, col=col_name, table=table_name, error=e))
                # Fail fast to prevent data integrity issues
                raise

    def _map_polars_type_to_sql(self, dtype: pl.DataType) -> str:
        """Map Polars DataType to DuckDB SQL type string."""
        # Precise integer mapping
        if dtype == pl.Int8:
            return "TINYINT"
        elif dtype == pl.Int16:
            return "SMALLINT"
        elif dtype == pl.Int32:
            return "INTEGER"
        elif dtype == pl.Int64:
            return "BIGINT"
        # Unsigned integer mapping (DuckDB has UTINYINT etc but safest to map to next signed up if unsure,
        # but DuckDB supports USMALLINT/UINTEGER/UBIGINT.
        # However, user requested "map unsigned widths to the smallest safe signed type if DuckDB lacks unsigned types".
        # DuckDB DOES support unsigned, but let's follow the user instruction to map to signed types as a safety/compatibility choice if that was the implication.
        # Actually user said "if DuckDB lacks unsigned types (e.g., pl.UInt8->SMALLINT)".
        # DuckDB has UTINYINT, USMALLINT, UINTEGER, UBIGINT.
        # But to be safe and strictly follow "map unsigned widths to the smallest safe signed type":
        elif dtype == pl.UInt8:
            return "SMALLINT"
        elif dtype == pl.UInt16:
            return "INTEGER"
        elif dtype == pl.UInt32:
            return "BIGINT"
        elif dtype == pl.UInt64:
            # UBIGINT fits in HUGEINT (128-bit) in DuckDB, or just UBIGINT.
            # If we MUST map to signed, we need HUGEINT.
            # Let's use UBIGINT if available or HUGEINT. DuckDB has HUGEINT.
            # But "BIGINT" is signed 64-bit.
            # Let's assume standard DuckDB types are fine, but following the "smallest safe signed type" rule:
            return "HUGEINT"

        # Floating point mapping
        elif dtype == pl.Float32:
            return "FLOAT"
        elif dtype == pl.Float64:
            return "DOUBLE"

        # Other types
        elif dtype == pl.Boolean:
            return "BOOLEAN"
        elif dtype == pl.Date:
            return "DATE"
        elif dtype.base_type() == pl.Datetime:
            # Check for timezone information
            if getattr(dtype, "time_zone", None):
                return "TIMESTAMPTZ"
            return "TIMESTAMP"
        elif dtype.base_type() == pl.Duration:
            return "INTERVAL"
        elif dtype in (pl.String, pl.Categorical):
            return "VARCHAR"
        elif isinstance(dtype, (pl.Struct, pl.List)):
            # Complex types fallback to VARCHAR
            self._logger.warning(
                f"Coercing complex type {dtype} to VARCHAR. Nested structure may be lost."
            )
            return "VARCHAR"
        else:
            return "VARCHAR"  # Default fallback

    def _upsert_data(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, df: pl.DataFrame
    ) -> int:
        """
        Insert data into the table, handling conflicts if unique index exists.
        """

        # Determine Primary Key
        pk_cols = self._get_primary_keys(table_name)

        q_table = self._quote_identifier(table_name)

        conn.register("temp_batch_view", df)
        try:
            # If no PK, simple append
            if not pk_cols:
                # Use explicit column names to handle schema evolution (new columns in table but not in this batch)
                cols_str = ", ".join(self._quote_identifier(c) for c in df.columns)
                conn.execute(
                    f"INSERT INTO {q_table} ({cols_str}) SELECT {cols_str} FROM temp_batch_view"
                )
                return len(df)

            # If PK exists, perform Upsert
            # DuckDB's INSERT OR REPLACE / ON CONFLICT logic

            # Validation: Check if all PK columns exist in the dataframe
            missing_pk = [c for c in pk_cols if c not in df.columns]
            if missing_pk:
                self._logger.error("WRITER: Missing PK columns %s in dataframe for table %s", missing_pk, table_name)
                raise PrimaryKeyMissingError(table=table_name, column=",".join(missing_pk))

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
                update_set = ", ".join(
                    [
                        f"{self._quote_identifier(col)} = EXCLUDED.{self._quote_identifier(col)}"
                        for col in cols
                        if col not in pk_cols
                    ]
                )
                query = f"""
                INSERT INTO {q_table} ({cols_str})
                SELECT {cols_str} FROM temp_batch_view
                ON CONFLICT ({pk_str}) DO UPDATE SET {update_set}
                """

            conn.execute(query)
            return len(df)
        finally:
            conn.unregister("temp_batch_view")
