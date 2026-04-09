from datetime import datetime, timezone

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.schema.mapper import SchemaMapper


def test_schema_mapper_casts_and_preserves_extra_columns() -> None:
    mapper = SchemaMapper()
    # Use sharadar_price schema which has a Date column
    df = pl.DataFrame(
        {
            "provider": ["sharadar"],
            "ticker": ["AAPL"],
            # Invalid date to trigger null on cast(strict=False)
            "date": ["not-a-date"],
            "open": [100.0],
            "extra_col": ["keep-me"],
        }
    )
    pkt = FramePacket(
        provider="sharadar",
        table="sharadar_price",
        frame=df,
        observed_at=datetime.now(timezone.utc),
    )
    out = mapper.normalize(packet=pkt)
    # date should exist and be cast to Date with null (strict=False)
    assert "date" in out.frame.columns
    assert out.frame.select(pl.col("date").is_null().sum()).item() == 1
    # extra column preserved
    assert "extra_col" in out.frame.columns
    # required columns from schema should exist
    for col in ("open", "close", "low", "high", "volume"):
        assert col in out.frame.columns


def test_schema_mapper_warns_when_schema_is_missing(caplog: pytest.LogCaptureFixture) -> None:
    mapper = SchemaMapper()
    df = pl.DataFrame({"id": [1], "value": ["x"]})
    pkt = FramePacket(
        provider="test",
        table="unknown_table",
        frame=df,
        observed_at=datetime.now(timezone.utc),
    )

    with caplog.at_level("WARNING"):
        out = mapper.normalize(packet=pkt)

    assert out.frame.equals(df)
    assert "no registered schema" in caplog.text.lower()
    counters = mapper.get_counters_and_reset()
    assert counters.get("schema_unknown_table_count", 0) == 1
