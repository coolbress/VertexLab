from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket
from vertex_forager.schema.mapper import SchemaMapper


def _packet(table: str, frame: pl.DataFrame) -> FramePacket:
    return FramePacket(
        provider="test",
        table=table,
        frame=frame,
        observed_at=datetime.now(tz=timezone.utc),
    )


def test_schema_mapper_non_strict_fills_missing_and_counts() -> None:
    # yfinance_info has many columns; provide only a subset to force fills
    df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"]})
    mapper = SchemaMapper(strict_validation=False)
    pkt = _packet("yfinance_info", df)
    out = mapper.normalize(pkt)

    assert out.frame.height == 1
    # All schema columns should exist after normalization
    assert "shortname" in out.frame.columns
    # Counters should record filled cells for missing columns
    counters = mapper.get_counters_and_reset()
    assert counters.get("schema_missing_cols_filled", 0) > 0


def test_schema_mapper_strict_raises_on_missing_columns() -> None:
    df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"]})
    mapper = SchemaMapper(strict_validation=True)
    pkt = _packet("yfinance_info", df)
    with pytest.raises(ValueError, match=r"Schema validation failed: missing required columns"):
        mapper.normalize(pkt)
