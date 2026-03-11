from datetime import datetime
import polars as pl
from vertex_forager.core.config import FramePacket
from vertex_forager.schema.mapper import SchemaMapper


def test_schema_mapper_casts_and_preserves_extra_columns() -> None:
    mapper = SchemaMapper()
    # Use sharadar_sep schema which has a Date column
    df = pl.DataFrame(
        {
            "provider": ["sharadar"],
            "ticker": ["AAPL"],
            # Intentionally provide an invalid date to trigger null on cast(strict=False)
            "date": ["not-a-date"],
            "open": [100.0],
            "extra_col": ["keep-me"],
        }
    )
    pkt = FramePacket(
        provider="sharadar",
        table="sharadar_sep",
        frame=df,
        observed_at=datetime.utcnow(),
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
