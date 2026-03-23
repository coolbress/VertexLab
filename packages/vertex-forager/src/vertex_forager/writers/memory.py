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

    def __init__(self, *, unique_key: list[str] | None = None, upsert_keys: list[str] | None = None) -> None:
        self._lock = threading.Lock()
        self._tables: dict[str, list[pl.DataFrame]] = {}
        # Backwards compatibility: accept either unique_key or upsert_keys
        keys = upsert_keys if upsert_keys is not None else unique_key
        self._upsert_keys: list[str] | None = list(keys) if keys else None
        self._counters: dict[str, int] = {}

    def set_unique_key(self, unique_key: list[str] | None) -> None:
        with self._lock:
            self._upsert_keys = list(unique_key) if unique_key else None

    def get_counters_and_reset(self) -> dict[str, int]:
        with self._lock:
            data = dict(self._counters)
            self._counters.clear()
            return data

    async def write(self, packet: FramePacket) -> WriteResult:
        """Append packet to the in-memory buffer.

        Thread-safe via threading lock.
        Applies deduplication if upsert_keys is configured, matching DuckDB semantics.
        """
        if packet.frame.is_empty():
            return WriteResult(table=packet.table, rows=0, partitions={})

        with self._lock:
            # Inline deduplication if upsert keys are set
            if self._upsert_keys:
                df = packet.frame
                subset = [c for c in self._upsert_keys if c in df.columns]
                if subset:
                    # Dedup within the new packet itself
                    df = df.unique(subset=subset, keep="last", maintain_order=True)

                existing_parts = self._tables.get(packet.table, [])
                if existing_parts and subset:
                    # Combine existing and new, then dedup
                    combined = pl.concat([*existing_parts, df], how="vertical", rechunk=False)
                    before = combined.height
                    deduped = combined.unique(subset=subset, keep="last", maintain_order=True)
                    dropped = before - deduped.height
                    if dropped > 0:
                        self._counters["inmem_dedup_dropped_rows"] = (
                            self._counters.get("inmem_dedup_dropped_rows", 0) + dropped
                        )
                    # Replace the buffer with the single deduped frame
                    self._tables[packet.table] = [deduped]
                else:
                    self._tables.setdefault(packet.table, []).append(df)
            else:
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

            # Optional in-memory dedup/upsert by unique key
            if self._upsert_keys:
                subset = [c for c in self._upsert_keys if c in df.columns]
                if subset:
                    before = df.height
                    # Keep last occurrence to approximate simple upsert semantics
                    df = df.unique(subset=subset, keep="last", maintain_order=True)
                    dropped = before - df.height
                    if dropped > 0:
                        self._counters["inmem_dedup_dropped_rows"] = (
                            self._counters.get("inmem_dedup_dropped_rows", 0) + dropped
                        )

            if sort_cols:
                # Only sort by columns that actually exist in the DataFrame
                valid_sort_cols = [c for c in sort_cols if c in df.columns]
                if valid_sort_cols:
                    df = df.sort(valid_sort_cols, maintain_order=True)

            return df
