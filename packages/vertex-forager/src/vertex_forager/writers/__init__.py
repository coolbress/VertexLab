from __future__ import annotations

from pathlib import Path
from typing import TypeAlias
from urllib.parse import urlparse

from vertex_forager.exceptions import InputError
from vertex_forager.writers.base import BaseWriter
from vertex_forager.writers.duckdb import DuckDBWriter
from vertex_forager.writers.memory import InMemoryBufferWriter

WriterInstance: TypeAlias = DuckDBWriter | InMemoryBufferWriter


def _duckdb_factory(uri: str) -> WriterInstance:
    """Create a DuckDB writer from a supported DuckDB URI."""
    parsed = urlparse(uri)
    if parsed.scheme != "duckdb":
        raise NotImplementedError(f"Writer for scheme '{parsed.scheme}' is not implemented")

    if parsed.netloc == ":memory:" and parsed.path in {"", "/"}:
        return DuckDBWriter(db_path=":memory:")

    if parsed.netloc == ".":
        relative_path = parsed.path.lstrip("/")
        if not relative_path:
            raise InputError("DuckDB relative URI must include a database file path")
        return DuckDBWriter(db_path=Path(relative_path))

    if parsed.netloc == "":
        if not parsed.path:
            raise InputError("DuckDB URI must include a database file path")

        normalized = parsed.path.lstrip("/")
        if not normalized:
            raise InputError("DuckDB URI must include a database file path")

        # `duckdb:///file.duckdb` is treated as a cwd-relative convenience form.
        if "/" not in normalized:
            return DuckDBWriter(db_path=Path(normalized))

        # Preserve absolute filesystem paths such as `duckdb:///tmp/file.duckdb`.
        return DuckDBWriter(db_path=Path(parsed.path))

    path_str = f"{parsed.netloc}{parsed.path}"
    return DuckDBWriter(db_path=Path(path_str))


def create_writer(connect_db: str | Path | None) -> WriterInstance:
    """
    Factory function to instantiate the appropriate Writer.

    Selection Logic:
    - **None**: Returns `InMemoryBufferWriter` (in-memory).
    - **String URI (duckdb://)**: Returns `DuckDBWriter`.
    - **String Path / Path object**: Returns `DuckDBWriter` (assumed file path).

    Args:
        connect_db: Connection string, Path object, or None.

    Returns:
        DuckDBWriter | InMemoryBufferWriter: An initialized writer instance.

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
            if parsed.scheme == "duckdb":
                return _duckdb_factory(connect_db)
            raise NotImplementedError(f"Writer for scheme '{parsed.scheme}' is not implemented")

    # 2. Plain String Path (No Scheme) -> Assume DuckDB
    # Previously mapped to HiveParquetWriter, now defaulting to DuckDB for simplicity
    return DuckDBWriter(db_path=Path(connect_db))


__all__ = ["BaseWriter", "DuckDBWriter", "InMemoryBufferWriter", "WriterInstance", "create_writer"]
