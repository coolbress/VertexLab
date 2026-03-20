from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import polars as pl

from vertex_forager.writers.base import BaseWriter, WriteResult

if TYPE_CHECKING:
    from vertex_forager.core.config import FramePacket


class InMemoryBufferWriter(BaseWriter):
    """In-memory writer for buffering results.

    Used when the user wants to get a DataFrame back directly without writing to disk.
    Accumulates all incoming packets in a dictionary of lists.

    Notes:
        - Not suitable for massive datasets that exceed memory.
        - Best used for small workloads or unit testing scenarios.

    Example:
        Collect and sort buffered frames for a table:
            writer = InMemoryBufferWriter()
            await writer.write(packet)
            df = writer.collect_table("price", sort_cols=["ticker", "date"])
    """

    def __init__(self, *, unique_key: list[str] | None = None) -> None:
        self._lock = threading.Lock()
        self._tables: dict[str, list[pl.DataFrame]] = {}
        self._unique_key: list[str] | None = list(unique_key) if unique_key else None
        self._counters: dict[str, int] = {}

    def set_unique_key(self, unique_key: list[str] | None) -> None:
        with self._lock:
            self._unique_key = list(unique_key) if unique_key else None

    def get_counters_and_reset(self) -> dict[str, int]:
        with self._lock:
            data = dict(self._counters)
            self._counters.clear()
            return data

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

        Example:
            df = writer.collect_table("price", sort_cols=["ticker", "date"])
            # df contains all buffered parts for 'price', optionally sorted
        """
        with self._lock:
            parts = self._tables.get(table) or []
            if not parts:
                return pl.DataFrame()

            df = parts[0] if len(parts) == 1 else pl.concat(parts, how="vertical", rechunk=False)

            if sort_cols:
                # Only sort by columns that actually exist in the DataFrame
                valid_sort_cols = [c for c in sort_cols if c in df.columns]
                if valid_sort_cols:
                    df = df.sort(valid_sort_cols)

            # Optional in-memory dedup/upsert by unique key
            if self._unique_key:
                subset = [c for c in self._unique_key if c in df.columns]
                if subset:
                    before = df.height
                    # Keep last occurrence to approximate simple upsert semantics
                    df = df.unique(subset=subset, keep="last", maintain_order=True)
                    dropped = before - df.height
                    if dropped > 0:
                        self._counters["inmem_dedup_dropped_rows"] = (
                            self._counters.get("inmem_dedup_dropped_rows", 0) + dropped
                        )

            return df
