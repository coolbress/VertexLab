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

    def collect_table(self, table: str) -> pl.DataFrame:
        """Concatenate all buffered parts for a table into a single DataFrame.

        Args:
            table: Table name (e.g., 'price_bars').

        Returns:
            pl.DataFrame: Combined data.
        """
        with self._lock:
            parts = self._tables.get(table) or []
            if not parts:
                return pl.DataFrame()
            
            # Copy list to avoid holding lock during concat
            parts = parts.copy()
        
        if len(parts) == 1:
            df = parts[0]
        else:
            df = pl.concat(parts, how="vertical", rechunk=False)

        # Sort by ticker and date if available (User Request)
        # This ensures deterministic output even if packets arrived out of order.
        sort_cols = []
        if "ticker" in df.columns:
            sort_cols.append("ticker")
        if "date" in df.columns:
            sort_cols.append("date")
        elif "filingdate" in df.columns:  # For SF2/SF3
            sort_cols.append("filingdate")
            
        if sort_cols:
            df = df.sort(sort_cols)
            
        return df
