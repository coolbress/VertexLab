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
    strict, pre-defined schemas before it reaches the Writer stage. This guarantees 
    type safety and structural consistency across different storage backends.

    Key Responsibilities:
    1. **Schema Lookup**: Retrieves the authoritative `TableSchema` for a given table name 
       from the central registry.
    2. **Type Casting**: forcibly casts all columns to the strict Polars data types 
       defined in the schema.
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
        It also ensures a standard 'date' column is available for downstream consumption.
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
        
        # Add standard 'date' column for analysis consistency
        frame = self._normalize_date_column(frame, table_schema.analysis_date_col)
        
        # Reorder columns to put unique key (PK) first for better readability
        frame = self._reorder_columns(frame, table_schema.unique_key)
        
        return packet.model_copy(update={"frame": frame})

    def _cast_to_schema(
        self, frame: pl.DataFrame, schema: dict[str, pl.DataType]
    ) -> pl.DataFrame:
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


    def _normalize_date_column(self, frame: pl.DataFrame, target_col: str | None) -> pl.DataFrame:
        """
        Ensure the designated analysis date column is renamed to 'date'.

        This method standardizes the temporal dimension for downstream analysis.
        The `target_col` is defined in the TableSchema (e.g., 'datekey' for SF1).

        Logic:
        1. If `target_col` is None, skip normalization.
        2. If `target_col` is "date", do nothing (already standard).
        3. If `frame` has a "date" column but it's NOT the target, rename it to "date_original" 
            to avoid collision and data loss.
        4. Rename `target_col` to "date".
        """
        if target_col is None:
            return frame

        if target_col == "date":
            return frame

        # Only rename 'date' if we are actually going to overwrite it with target_col
        if target_col in frame.columns:
            if "date" in frame.columns:
                frame = frame.rename({"date": "date_original"})
            return frame.rename({target_col: "date"})

        return frame
