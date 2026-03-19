from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from vertex_forager.schema.registry import get_table_schema

if TYPE_CHECKING:
    from vertex_forager.core.config import FramePacket


class SchemaMapper:
    """
    Core component responsible for data normalization and schema enforcement.

    The SchemaMapper ensures that all data flowing through the pipeline conforms to
    pre-defined schemas before it reaches the Writer stage. This guarantees
    type safety and structural consistency across different storage backends.

    Key Responsibilities:
    1. **Schema Lookup**: Retrieves the authoritative `TableSchema` for a given table name
        from the central registry.
    2. **Type Casting**: casts columns to the Polars data types defined in the schema
        using non-strict casting (strict=False) to allow nulls on failure.
    3. **Missing Column Handling**: Automatically adds missing schema columns with `null`
        values to ensure downstream systems receive complete records.
    4. **Column Ordering**: Reorders columns to match the canonical schema definition.

    Usage:
        mapper = SchemaMapper()
        normalized_packet = mapper.normalize(raw_packet)
    """

    def normalize(self, packet: FramePacket) -> FramePacket:
        """
        Enforce schema conformance on a data packet.

        This method transforms a raw DataFrame into a schema-compliant DataFrame.
        If a schema is registered, the frame is cast to declared types and columns
        are reordered. When `analysis_date_col` is set on the schema and present
        in `frame.columns`, that column is cast to the schema type (strict=False)
        and the frame is sorted by it. No new column is created if the target
        `analysis_date_col` is absent.

        If no schema is registered for the table, the packet is returned strictly as-is.

        Args:
            packet: Input packet containing potentially raw/untyped data.

        Returns:
            FramePacket: A new packet containing the normalized DataFrame.
        """
        table_schema = get_table_schema(packet.table)
        if table_schema is None or packet.frame.is_empty():
            return packet

        frame = self._cast_to_schema(packet.frame, table_schema.schema)

        if table_schema.analysis_date_col is not None:
            target_col = table_schema.analysis_date_col
            target_dtype = table_schema.schema.get(target_col, pl.Date)
            if target_col in frame.columns:
                frame = frame.with_columns(pl.col(target_col).cast(target_dtype, strict=False).alias(target_col))
                frame = frame.sort(target_col)

        # If original had date, _cast_to_schema preserved it (or cast it).
        # We don't overwrite it.

        # Reorder columns to put unique key (PK) first for better readability
        frame = self._reorder_columns(frame, table_schema.unique_key)

        return packet.model_copy(update={"frame": frame})

    def _cast_to_schema(self, frame: pl.DataFrame, schema: dict[str, pl.DataType | type[pl.DataType]]) -> pl.DataFrame:
        """
        Internal helper to align a DataFrame with the target schema.

        Strategies:
        - **Existing Columns**: Cast to target type (strict=False to allow nulls on failure).
        - **Missing Columns**: Create with null values.
        - **Extra Columns**: Preserved and appended after schema columns.
        """
        cols = set(frame.columns)
        exprs: list[pl.Expr] = []

        # 1. Handle Schema Columns (Cast or Create)
        for name, dtype in schema.items():
            if name not in cols:
                # Missing column: Create as null
                exprs.append(pl.lit(None).cast(dtype).alias(name))
            else:
                # Existing column: Cast
                exprs.append(pl.col(name).cast(dtype, strict=False).alias(name))

        # 2. Apply Projections
        # Note: We do NOT filter out extra columns. They are preserved.
        # This allows the schema to define the "required core" while allowing extensibility.
        out = frame.with_columns(exprs)

        # 3. Reorder Columns
        # Schema columns come first in defined order, followed by any extra columns found in input
        ordered_cols = list(schema.keys()) + [c for c in out.columns if c not in schema]
        return out.select(ordered_cols)

    def _reorder_columns(self, frame: pl.DataFrame, unique_key: tuple[str, ...]) -> pl.DataFrame:
        """
        Reorder columns to prioritize unique keys (PK) at the beginning.

        Order: [Unique Key Columns] + [Remaining Columns]
        """
        if not unique_key:
            return frame

        pk_cols = [col for col in unique_key if col in frame.columns]
        other_cols = [col for col in frame.columns if col not in pk_cols]

        return frame.select(pk_cols + other_cols)
