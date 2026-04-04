from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from vertex_forager.core.quality import NoDuplicateRows, NoFutureDates, NoNegativePrices


def test_no_negative_prices_all_valid() -> None:
    df = pl.DataFrame(
        {
            "open": [10.0, 1.0, 3.0],
            "high": [11.0, 2.0, 4.0],
            "low": [9.0, 0.5, 2.5],
            "close": [10.5, 1.5, 3.5],
        }
    )
    rule = NoNegativePrices()
    violations = rule.validate(df)
    assert violations == []


def test_no_negative_prices_detects_and_ignores_missing_columns() -> None:
    df = pl.DataFrame(
        {
            "open": [10.0, -1.0, 3.0],
            "close": [1.0, 2.0, -3.0],
            "volume": [100, 200, 300],
        }
    )
    rule = NoNegativePrices(price_columns=["open", "close", "high"])
    violations = rule.validate(df)
    assert len(violations) == 2
    assert "open" in violations[0] or "open" in violations[1]
    assert "close" in violations[0] or "close" in violations[1]


def test_no_future_dates_handles_date_and_datetime_columns() -> None:
    dt = datetime.now(timezone.utc).replace(microsecond=0)
    today = dt.date()
    now = dt
    df = pl.DataFrame(
        {
            "date": [today, today + timedelta(days=1)],
            "observed_at": [now, now + timedelta(hours=1)],
        }
    ).with_columns(pl.col("observed_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")))
    rule = NoFutureDates(date_columns=["date", "observed_at"])
    rule._current_time = lambda: now.astimezone(timezone.utc)  # type: ignore[method-assign]
    violations = rule.validate(df)
    assert len(violations) == 2
    assert any("date" in v for v in violations)
    assert any("observed_at" in v for v in violations)


def test_no_future_dates_boundary_equal_to_now() -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    df = pl.DataFrame(
        {
            "date": [now.date()],
            "observed_at": [now],
        }
    ).with_columns(pl.col("observed_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")))
    rule = NoFutureDates(date_columns=["date", "observed_at"])
    violations = rule.validate(df)
    assert violations == []


def test_no_future_dates_skips_non_date_columns() -> None:
    df = pl.DataFrame({"name": ["a", "b"], "value": [1, 2]})
    rule = NoFutureDates(date_columns=["name", "value"])
    violations = rule.validate(df)
    assert violations == []


def test_no_future_dates_date_uses_configured_local_timezone() -> None:
    now_utc = datetime(2025, 1, 2, 0, 30, tzinfo=timezone.utc)
    df = pl.DataFrame({"date": [now_utc.date()]})
    utc_rule = NoFutureDates(date_columns=["date"], timezone="UTC")
    utc_rule._current_time = lambda: now_utc.astimezone(timezone.utc)  # type: ignore[method-assign]
    violations_without_local_tz = utc_rule.validate(df)

    local_rule = NoFutureDates(date_columns=["date"], timezone="America/New_York")
    local_rule._current_time = lambda: now_utc.astimezone(ZoneInfo("America/New_York"))  # type: ignore[method-assign]
    violations_with_local_tz = local_rule.validate(df)

    assert violations_without_local_tz == []
    assert violations_with_local_tz == ["Column 'date' contains 1 future dates"]


def test_no_duplicate_rows_subset_and_all_columns() -> None:
    df = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "date": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-02"],
            "close": [100.0, 101.0, 200.0, 210.0],
        }
    )
    subset_rule = NoDuplicateRows(subset=["ticker", "date"])
    subset_violations = subset_rule.validate(df)
    assert len(subset_violations) == 1
    assert "duplicate rows" in subset_violations[0]
    assert "on columns" in subset_violations[0]

    all_rule = NoDuplicateRows()
    all_violations = all_rule.validate(
        pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL", "MSFT"],
                "date": ["2025-01-01", "2025-01-01", "2025-01-02"],
            }
        )
    )
    assert len(all_violations) == 1
    assert "across all columns" in all_violations[0]


def test_no_duplicate_rows_subset_missing_columns_is_noop() -> None:
    df = pl.DataFrame({"ticker": ["AAPL", "AAPL"]})
    rule = NoDuplicateRows(subset=["date"])
    violations = rule.validate(df)
    assert violations == []
