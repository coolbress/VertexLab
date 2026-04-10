from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from vertex_forager.core.quality import DataQualityRule


@dataclass(frozen=True, slots=True)
class TableSchema:
    """
    Internal storage contract for a normalized table.

    Attributes:
        table: Canonical table name (e.g., "sharadar_price").
        schema: Mapping of normalized column names to Polars dtypes.
        unique_key: Tuple of columns that must exist and act as the logical
            primary key for deduplication and upsert validation.
        analysis_date_col: Optional canonical date-like column used after fetch
            for internal ordering and analysis. This is an internal data-model
            concern, not a vendor request parameter.
        flexible_schema: Whether extra columns may be preserved when frames with
            slightly different shapes are merged.
        quality_rules: Data quality rules evaluated against the normalized
            internal schema.
    """

    table: str
    schema: dict[str, pl.DataType | type[pl.DataType]]
    unique_key: tuple[str, ...] = ()
    analysis_date_col: str | None = None
    flexible_schema: bool = False
    quality_rules: tuple[DataQualityRule, ...] = ()

    def __post_init__(self) -> None:
        schema_columns = set(self.schema)
        missing_unique_key = sorted(column for column in self.unique_key if column not in schema_columns)
        if missing_unique_key:
            raise ValueError(f"TableSchema '{self.table}' unique_key columns missing from schema: {missing_unique_key}")
        if self.analysis_date_col is not None and self.analysis_date_col not in schema_columns:
            raise ValueError(
                f"TableSchema '{self.table}' analysis_date_col '{self.analysis_date_col}' missing from schema"
            )
        missing_rule_columns = sorted(
            {column for rule in self.quality_rules for column in rule.required_columns if column not in schema_columns}
        )
        if missing_rule_columns:
            raise ValueError(
                f"TableSchema '{self.table}' quality rule columns missing from schema: {missing_rule_columns}"
            )


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """
    External vendor-facing contract for a fetchable dataset.

    Attributes:
        name: Provider-local dataset identifier used by routers and clients.
        schema: Internal target schema that the fetched payload must normalize
            into.
        endpoint: Provider endpoint or method alias used to request the dataset
            from the vendor.
        date_filter_col: Optional vendor/request-side field name used when a
            caller provides start/end date filters. This controls how date
            limits are translated into outbound request parameters and may
            differ from `TableSchema.analysis_date_col`.
    """

    name: str
    schema: TableSchema
    endpoint: str
    date_filter_col: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("DatasetSpec.name must be non-empty")
        if not self.endpoint.strip():
            raise ValueError(f"DatasetSpec '{self.name}' endpoint must be non-empty")
        if self.date_filter_col is None and self.schema.analysis_date_col is not None:
            object.__setattr__(self, "date_filter_col", self.schema.analysis_date_col)
        if self.date_filter_col is not None and self.date_filter_col not in self.schema.schema:
            raise ValueError(
                f"DatasetSpec '{self.name}' date_filter_col '{self.date_filter_col}' "
                f"missing from schema '{self.schema.table}'"
            )
