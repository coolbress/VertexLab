from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from vertex_forager.schema.mapper import SchemaMapper


def test_schema_mapper_non_strict_fills_missing_and_counts(pkt_factory) -> None:
    # yfinance_info has many columns; provide only a subset to force fills
    df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"]})
    mapper = SchemaMapper(strict_validation=False)
    pkt = pkt_factory("yfinance_info", df)
    out = mapper.normalize(pkt)

    assert out.frame.height == 1
    # All schema columns should exist after normalization
    assert "shortname" in out.frame.columns
    # Counters should record filled cells for missing columns
    counters = mapper.get_counters_and_reset()
    assert counters.get("schema_missing_cols_filled", 0) > 0


def test_schema_mapper_strict_raises_on_missing_columns(pkt_factory) -> None:
    df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"]})
    mapper = SchemaMapper(strict_validation=True)
    pkt = pkt_factory("yfinance_info", df)
    with pytest.raises(ValueError, match=r"Schema validation failed: missing required columns"):
        mapper.normalize(pkt)


def test_schema_mapper_strict_raises_on_type_mismatch(pkt_factory) -> None:
    # Use yfinance_financials schema; provide all required columns but make 'value' incompatible (string)
    df = pl.DataFrame(
        {
            "date": [date(2024, 1, 1)],
            "ticker": ["AAPL"],
            "provider": ["yfinance"],
            "period": ["annual"],
            "metric": ["net_income"],
            "value": ["not_a_number"],  # type mismatch for Float64
            "fetched_at": [datetime.now(tz=timezone.utc)],
        }
    )
    mapper = SchemaMapper(strict_validation=True)
    pkt = pkt_factory("yfinance_financials", df)
    with pytest.raises(ValueError, match=r"Schema validation failed: type casting error"):
        mapper.normalize(pkt)


def test_schema_mapper_strict_ok_when_columns_and_types_valid(pkt_factory) -> None:
    df = pl.DataFrame(
        {
            "date": [date(2024, 1, 1)],
            "ticker": ["AAPL"],
            "provider": ["yfinance"],
            "period": ["annual"],
            "metric": ["net_income"],
            "value": [123.45],
            "fetched_at": [datetime.now(tz=timezone.utc)],
        }
    )
    mapper = SchemaMapper(strict_validation=True)
    pkt = pkt_factory("yfinance_financials", df)
    out = mapper.normalize(pkt)
    assert out.frame.height == 1
    counters = mapper.get_counters_and_reset()
    assert counters.get("schema_missing_cols_filled", 0) == 0
