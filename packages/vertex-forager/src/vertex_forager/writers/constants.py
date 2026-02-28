from __future__ import annotations

from typing import Final

WR_LOG_PREFIX: Final[str] = "WRITER"
DK_LOG_PREFIX: Final[str] = "DUCKDB"

LOG_START_SYNC_WRITE: Final[str] = "{prefix}: Starting sync write for {packets} packets ({rows} rows)"
LOG_FINISH_SYNC_WRITE: Final[str] = "{prefix}: Finished sync write in {seconds:.3f}s. Results: {results}"
LOG_SCHEMA_MISMATCH: Final[str] = "{prefix}: Schema mismatch for {table}: {error}. Falling back to diagonal concat"
LOG_MISSING_PK_COL: Final[str] = "{prefix}: Missing PK column '{col}' for table '{table}'. Columns={columns}"
LOG_NULLS_PK_COL: Final[str] = "{prefix}: Nulls detected in PK column '{col}' for table '{table}'. rows={rows}, columns={columns}"
LOG_FAILED_BATCH: Final[str] = "{prefix}: Failed to write batch for {table}: {error}"
LOG_VALIDATION_ERROR: Final[str] = "{prefix}: Validation error writing batch for {table}: {error}"
LOG_NO_CONN_SKIP_COMPACT: Final[str] = "{prefix}: No DuckDB connection; skipping compaction"
LOG_COMPACTING: Final[str] = "{prefix}: Compacting DuckDB database..."
LOG_COMPACT_DONE: Final[str] = "{prefix}: Compaction completed."
LOG_CREATE_TABLE: Final[str] = "{prefix}: Creating table '{table}' in DuckDB"
LOG_UNIQUE_INDEX_CREATED: Final[str] = "{prefix}: Created UNIQUE INDEX on {table}({pk_str})"
LOG_UNIQUE_INDEX_FAIL: Final[str] = "{prefix}: Failed to create unique index on {table}: {error}"
LOG_SKIP_UNIQUE_INDEX_MISSING_PK: Final[str] = "{prefix}: Skipping UNIQUE INDEX for {table}: Missing PK columns {missing} in dataframe"
LOG_SCHEMA_EVOLUTION_ADDING: Final[str] = "{prefix}: Schema evolution: Adding {count} new columns to {table}"
LOG_ADD_COLUMN: Final[str] = "{prefix}: Adding column {col} ({sql_type}) to {table}"
LOG_ADD_COLUMN_FAIL: Final[str] = "{prefix}: Failed to add column {col} to {table}: {error}"
LOG_CLOSE_CONN_WARNING: Final[str] = "{prefix}: Error closing DuckDB connection: {error}"
LOG_CORRELATION_SUMMARY: Final[str] = "{prefix}: Correlation summary: trace_ids={trace_ids_count}, request_ids={request_ids_count}"
LOG_CORRELATION_SAMPLES: Final[str] = "{prefix}: Correlation samples: trace_ids={trace_ids}, request_ids={request_ids}"
