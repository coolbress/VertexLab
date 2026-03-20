"""
Vertex Forager Writers Base Module.

This module defines the abstract base classes and common types for data writers in the
Vertex Forager pipeline. Writers are responsible for persisting normalized data frames
to various storage backends (e.g., DuckDB, In-Memory).

Classes:
    WriteResult: Data class capturing the result of a write operation.
    BaseWriter: Abstract base class defining the writer interface.

Usage:
    class MyWriter(BaseWriter):
        async def write(self, packet: FramePacket) -> WriteResult:
            ...

Notes:
    All writers must implement the `write` and `write_bulk` methods and handle
    async context management.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    import polars as pl

    from vertex_forager.core.config import FramePacket


class WriteResult(BaseModel):
    """
    Writer write result.

    Attributes:
        table (str): The target table name written to.
        rows (int): Number of rows written.
        partitions (Mapping[str, str]): Partition key/value pairs created or updated (defaults to empty dict).
    """

    table: str
    rows: int
    partitions: dict[str, str] = Field(default_factory=dict)


class BaseWriter(ABC):
    """
    Abstract Base Class for Data Writers.

    The Writer component acts as the final destination in the data pipeline, responsible
    for persisting normalized `FramePacket`s (Polars DataFrames) to a durable storage backend.

    Key Responsibilities:
    1. **Persistence**: Efficiently writing data to disk (DuckDB, Parquet) or memory.
    2. **Concurrency Safety**: Managing thread/async safety for storage engines that require
       single-writer access (e.g., DuckDB).
    3. **Bulk Processing**: Implementing `write_bulk` to optimize throughput by reducing
       transaction overhead.
    4. **Resource Management**: Handling connections, file handles, and proper cleanup via
       async context managers (`__aenter__`, `__aexit__`).

    Design Principles:
    - **Schema-Agnostic**: Writers receive already-normalized data. They trust the upstream
      `SchemaMapper` and do not perform schema validation.
    - **Fail-Fast**: Errors during write operations should propagate immediately to stop
      the pipeline or trigger retry logic.
    """

    @abstractmethod
    async def write(self, packet: FramePacket) -> WriteResult:
        """
        Persist a single data packet.

        Args:
            packet: The normalized data packet containing a Polars DataFrame.

        Returns:
            WriteResult: Summary including table name and written row count.

        Raises:
            polars.exceptions.ComputeError: On data processing errors.
            pydantic.ValidationError: On schema validation errors.
            duckdb.Error: On database specific errors (if applicable).
        """

    async def write_bulk(self, packets: list[FramePacket]) -> list[WriteResult]:
        """
        Persist a bulk of data packets.

        Default implementation iterates `write()`.
        Subclasses should override this for transactional/bulk optimization.

        Args:
            packets: List of data packets.

        Returns:
            list[WriteResult]: List of results for each packet.
        """
        results = []
        for packet in packets:
            results.append(await self.write(packet))
        return results

    async def flush(self) -> None:
        """Flush any buffered data to storage.

        Default implementation does nothing. Override if buffering is used.
        """
        return None

    async def close(self) -> None:
        """Close any open resources (connections, files).

        Default implementation does nothing. Override if resources need cleanup.
        """
        return None

    async def __aenter__(self) -> BaseWriter:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Async context manager exit.

        Ensures resources are closed even if an error occurs.
        """
        await self.close()

    def collect_table(self, table_name: str, sort_cols: list[str] | None = None) -> pl.DataFrame:
        """
        Collect table data in memory for in-memory writers.

        Args:
            table_name (str): The name of the table to collect.
            sort_cols (list[str] | None): Optional list of columns to sort by.

        Returns:
            pl.DataFrame: The collected DataFrame.

        Raises:
            NotImplementedError: If the writer does not support in-memory collection.
        """
        raise NotImplementedError("collect_table is only supported by in-memory writers")
