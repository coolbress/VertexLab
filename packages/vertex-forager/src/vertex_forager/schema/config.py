from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


@dataclass(frozen=True, slots=True)
class TableSchema:
    """
    Definition of a table's structural constraints.

    Attributes:
        table: Canonical table name (e.g., "sharadar_sep").
        schema: Mapping of column names to Polars DataTypes.
        unique_key: Tuple of column names that form the primary key
            (for deduplication).
        analysis_date_col: Optional timestamp/analysis column used for
            time-based processing. Defaults to None.
        flexible_schema: Whether schema is permissive to extra/unknown
            fields. Defaults to False.
    """

    table: str
    schema: dict[str, pl.DataType | type[pl.DataType]]
    unique_key: tuple[str, ...] = ()
    analysis_date_col: str | None = None
    flexible_schema: bool = False
