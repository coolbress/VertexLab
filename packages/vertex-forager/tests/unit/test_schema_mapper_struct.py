from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.core.quality import NoFutureDates
from vertex_forager.schema.config import DatasetSpec, TableSchema
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.schema.registry import get_dataset_spec


def test_schema_mapper_nested_struct_cast_and_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    struct_dtype = pl.Struct({"a": pl.Int64, "b": pl.String})
    schema = TableSchema(
        table="tmp_struct_tbl",
        schema={
            "provider": pl.String,
            "meta": struct_dtype,
        },
        unique_key=("provider",),
        analysis_date_col=None,
    )

    # Monkeypatch mapper module's imported get_table_schema so normalize() uses the stub
    import vertex_forager.schema.mapper as mapper_mod
    monkeypatch.setattr(
        mapper_mod,
        "get_table_schema",
        lambda name: schema if name == "tmp_struct_tbl" else None,
    )

    # Input includes mixed/partial types to exercise non-strict casting
    data = [
        {"provider": "test", "meta": {"a": 1, "b": "x"}},           # already matches
        {"provider": "test", "meta": {"a": "2", "b": 3}},  # needs casting
        {"provider": "test", "meta": {"a": None, "b": None}},       # nulls
    ]
    df = pl.DataFrame(data)

    pkt = FramePacket(
        provider="test",
        table="tmp_struct_tbl",
        frame=df,
        observed_at=datetime.now(timezone.utc),
    )

    out = SchemaMapper().normalize(packet=pkt)

    # Column exists and has Struct dtype
    assert "meta" in out.frame.columns
    assert isinstance(out.frame.schema["meta"], pl.Struct)

    # The struct fields should be present
    # Types reflect schema under strict=False casting
    f_a = out.frame.select(pl.col("meta").struct.field("a")).to_series()
    f_b = out.frame.select(pl.col("meta").struct.field("b")).to_series()
    assert f_a.dtype == pl.Int64
    assert f_b.dtype == pl.String
    assert f_a.to_list() == [1, 2, None]
    assert f_b.to_list() == ["x", "3", None]


def test_table_schema_rejects_missing_unique_key_column() -> None:
    with pytest.raises(ValueError, match="unique_key columns missing"):
        TableSchema(
            table="tmp_invalid_pk",
            schema={"ticker": pl.String},
            unique_key=("provider",),
        )


def test_table_schema_rejects_missing_analysis_date_column() -> None:
    with pytest.raises(ValueError, match="analysis_date_col"):
        TableSchema(
            table="tmp_invalid_analysis_date",
            schema={"ticker": pl.String},
            analysis_date_col="date",
        )


def test_table_schema_rejects_missing_quality_rule_column() -> None:
    with pytest.raises(ValueError, match="quality rule columns missing"):
        TableSchema(
            table="tmp_invalid_quality_rule",
            schema={"ticker": pl.String},
            quality_rules=(NoFutureDates(["date"]),),
        )


def test_dataset_spec_rejects_missing_date_filter_column() -> None:
    schema = TableSchema(
        table="tmp_dataset_spec",
        schema={"ticker": pl.String},
    )

    with pytest.raises(ValueError, match="date_filter_col"):
        DatasetSpec(
            name="tmp_dataset",
            schema=schema,
            endpoint="history",
            date_filter_col="date",
        )


def test_registry_returns_dataset_spec() -> None:
    spec = get_dataset_spec("sharadar", "price")

    assert spec is not None
    assert spec.name == "price"
    assert spec.schema.table == "sharadar_sep"
