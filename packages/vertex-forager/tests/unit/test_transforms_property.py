from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import re

from hypothesis import given
from hypothesis import strategies as st
import polars as pl
import pytest

from vertex_forager.routers.transforms import normalize_columns, parse_date_range


@given(st.lists(st.text(min_size=0, max_size=20), min_size=1, max_size=10))
def test_normalize_columns_property(names: list[str]) -> None:
    df = pl.DataFrame({n or "": [] for n in names})
    out = normalize_columns(df)
    assert len(out.columns) == len(df.columns)
    # All lowercase snake_case
    assert all(re.fullmatch(r"[a-z0-9_]+", c) for c in out.columns)
    # Unique names
    assert len(set(out.columns)) == len(out.columns)


@given(
    st.dates(
        min_value=datetime(1970, 1, 1).date(),
        max_value=datetime(2030, 12, 31).date(),
    ),
    st.integers(min_value=0, max_value=3650),
)
def test_parse_date_range_property(start_date: date, delta_days: int) -> None:
    start_str = start_date.strftime("%Y-%m-%d")
    end_dt = datetime(
        start_date.year,
        start_date.month,
        start_date.day,
        tzinfo=timezone.utc,
    ) + timedelta(days=delta_days)
    end_str = end_dt.strftime("%Y-%m-%d")
    rng = parse_date_range(start_str, end_str)
    assert rng is not None
    s, e = rng
    assert e >= s


def test_parse_date_range_invalid_raises() -> None:
    with pytest.raises(ValueError, match=r"End date is earlier than start date"):
        _ = parse_date_range("2024-01-10", "2024-01-01")
