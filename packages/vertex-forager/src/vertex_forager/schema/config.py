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
        analysis_date_col: Optional column name that serves as the primary time dimension for analysis.
                           This is metadata only and does not trigger automatic column renaming.
                           Downstream query engines should use this column for time-based filtering/sorting.
    """
    table: str
    schema: dict[str, pl.DataType]
    unique_key: tuple[str, ...] = ()
    analysis_date_col: str | None = None


