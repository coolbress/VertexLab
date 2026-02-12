from __future__ import annotations

import threading

import polars as pl

from vertex_forager.core.config import FramePacket
from vertex_forager.writers.base import BaseWriter, WriteResult


class InMemoryBufferWriter(BaseWriter):
    """In-memory writer for buffering results.

    Used when the user wants to get a DataFrame back directly without writing to disk.
    Accumulates all incoming packets in a dictionary of lists.

    Note: Not suitable for massive datasets that exceed memory.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tables: dict[str, list[pl.DataFrame]] = {}

    async def write(self, packet: FramePacket) -> WriteResult:
        """Append packet to the in-memory buffer.

        Thread-safe via threading lock.
        """
        if packet.frame.is_empty():
            return WriteResult(table=packet.table, rows=0, partitions={})

        with self._lock:
            self._tables.setdefault(packet.table, []).append(packet.frame)

        return WriteResult(table=packet.table, rows=packet.frame.height, partitions={})

    def collect_table(self, table: str, sort_cols: list[str] | None = None) -> pl.DataFrame:
        """Concatenate all buffered parts for a table into a single DataFrame.

        Args:
            table: Table name (e.g., 'price_bars').
            sort_cols: Optional list of columns to sort by (e.g., from schema unique_key).

        Returns:
            pl.DataFrame: Combined data.
        """
        with self._lock:
            parts = self._tables.get(table) or []
            if not parts:
                return pl.DataFrame()
            
            if len(parts) == 1:
                df = parts[0]
            else:
                df = pl.concat(parts, how="vertical", rechunk=False)

            if sort_cols:
                # Only sort by columns that actually exist in the DataFrame
                valid_sort_cols = [c for c in sort_cols if c in df.columns]
                if valid_sort_cols:
                    df = df.sort(valid_sort_cols)
                
            return df
