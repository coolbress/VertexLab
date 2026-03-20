from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.schema.config import TableSchema
from vertex_forager.schema.mapper import SchemaMapper


def test_schema_mapper_nested_struct_cast_and_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Define a temporary schema with a nested struct column
    struct_dtype = pl.Struct({"a": pl.Int64, "b": pl.Utf8})
    schema = TableSchema(
        table="tmp_struct_tbl",
        schema={
            "provider": pl.Utf8,
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
    assert f_b.dtype == pl.Utf8
    # Values: "2" -> 2; 1 -> 1; None; 3 -> "3" for b
    assert f_a.to_list() == [1, 2, None]
    assert f_b.to_list() == ["x", "3", None]
