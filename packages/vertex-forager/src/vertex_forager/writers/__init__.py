from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from vertex_forager.core.registries import writers as writer_registry
from vertex_forager.writers.base import BaseWriter
from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.writers.memory import InMemoryBufferWriter


def _duckdb_factory(uri: str) -> BaseWriter:
    """Create a DuckDB writer from URI (duckdb:///path/to/db.duckdb)."""
    # Simple parsing: remove scheme prefix
    path_str = uri.replace("duckdb://", "")
    return DuckDBWriter(db_path=Path(path_str))


# Register built-in writers
writer_registry.register("duckdb", _duckdb_factory)


def create_writer(connect_db: str | Path | None) -> BaseWriter:
    """
    Factory function to instantiate the appropriate Writer.

    Selection Logic:
    - **None**: Returns `InMemoryBufferWriter` (in-memory).
    - **String URI (duckdb://)**: Returns `DuckDBWriter`.
    - **String Path / Path object**: Returns `DuckDBWriter` (assumed file path).

    Args:
        connect_db: Connection string, Path object, or None.

    Returns:
        BaseWriter: An initialized writer instance.

    Raises:
        NotImplementedError: If a URI scheme is unknown.
    """
    if connect_db is None:
        return InMemoryBufferWriter()

    # Treat explicit Path objects as DuckDB paths (Standard Default)
    if isinstance(connect_db, Path):
        return DuckDBWriter(db_path=connect_db)

    # Handle String Input
    # Fix: Only treat as URI if it contains '://' to avoid misinterpreting Windows paths
    if isinstance(connect_db, str) and "://" in connect_db:
        parsed = urlparse(connect_db)

        # 1. URI with Scheme (e.g., duckdb://)
        if parsed.scheme:
            try:
                factory = writer_registry.get(parsed.scheme)
                return factory(connect_db)
            except NotImplementedError:
                raise NotImplementedError(f"Writer for scheme '{parsed.scheme}' is not implemented") from None

    # 2. Plain String Path (No Scheme) -> Assume DuckDB
    # Previously mapped to HiveParquetWriter, now defaulting to DuckDB for simplicity
    return DuckDBWriter(db_path=Path(connect_db))


__all__ = ["BaseWriter", "DuckDBWriter", "InMemoryBufferWriter", "create_writer"]
