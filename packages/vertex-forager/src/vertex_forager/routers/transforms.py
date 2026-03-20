"""Provider-agnostic router utilities.

Defines shared normalization helpers used by router implementations. While
these functions are provider-agnostic, they are scoped to the routers layer
to keep core surface minimal and emphasize the adapter responsibility.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from vertex_forager.core.config import ParseResult


def add_provider_metadata(
    provider: str,
    *,
    frame: pl.DataFrame,
    observed_at: datetime,
) -> pl.DataFrame:
    """Add provider metadata columns to a DataFrame.

    Args:
        provider: Provider identifier (e.g., 'sharadar', 'yfinance').
        frame: Input DataFrame to add metadata to.
        observed_at: Timestamp when the data was fetched.

    Returns:
        pl.DataFrame: DataFrame with added provider and fetched_at columns.
    """
    return frame.with_columns(
        [
            pl.lit(provider).alias("provider"),
            pl.lit(observed_at).alias("fetched_at"),
        ]
    )


def check_empty_response(
    *,
    payload: bytes | None = None,
    frame: pl.DataFrame | None = None,
) -> ParseResult | None:
    """Return an empty ParseResult when payload or frame is empty; otherwise None."""
    if payload is not None and not payload:
        return ParseResult(packets=[], next_jobs=[])
    if frame is not None and frame.is_empty():
        return ParseResult(packets=[], next_jobs=[])
    return None


def parse_date_range(
    start_date: str | None,
    end_date: str | None,
) -> tuple[datetime, datetime] | None:
    """Parse YYYY-MM-DD date strings into UTC datetimes; validate that end >= start."""
    if not start_date:
        return None
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = datetime.now(timezone.utc)
        if end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid date format: start_date={start_date!r}, end_date={end_date!r}") from None
    if end < start:
        raise ValueError("End date is earlier than start date")
    return start, end


def normalize_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Standardize column names to lowercase snake_case and ensure uniqueness."""
    import re

    normalized = [(re.sub(r"[^0-9a-zA-Z]+", "_", c).lower().strip("_") or "column") for c in frame.columns]
    seen: dict[str, int] = {}
    unique_names: list[str] = []
    for name in normalized:
        count = seen.get(name, 0)
        if count == 0:
            unique_names.append(name)
            seen[name] = 1
        else:
            new_name = f"{name}_{count}"
            while new_name in seen:
                count += 1
                new_name = f"{name}_{count}"
            unique_names.append(new_name)
            seen[name] = count + 1
            seen[new_name] = 1
    return frame.rename(dict(zip(frame.columns, unique_names, strict=True)))
