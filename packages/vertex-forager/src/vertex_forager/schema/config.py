from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, slots=True)
class TableSchema:
    """
    Definition of a table's structural constraints.

    Attributes:
        table: Canonical table name (e.g., "sharadar_sep").
        schema: Mapping of column names to Polars DataTypes.
        unique_key: Tuple of column names that form the primary key (for deduplication).
        analysis_date_col: Optional column name used as the analysis/timestamp column for time-based processing. Defaults to None.
    """
    table: str
    schema: dict[str, pl.DataType]
    unique_key: tuple[str, ...] = ()
    analysis_date_col: str | None = None


