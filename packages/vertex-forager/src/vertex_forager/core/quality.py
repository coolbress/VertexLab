"""Data quality validation rules and utilities."""

import asyncio
import re
from typing import Any, Literal, Protocol, cast

import polars as pl

from vertex_forager.core.config import RunResult
from vertex_forager.schema.registry import get_table_schema


class DataQualityRule(Protocol):
    """Protocol for data quality validation rules.

    Rules should implement this protocol to validate DataFrame content
    and return violation messages for any quality issues found.
    """

    def validate(self, df: pl.DataFrame) -> list[str]:
        """Validate DataFrame content for quality issues.

        Args:
            df: DataFrame to validate

        Returns:
            list[str]: List of violation messages for any quality issues found.
            Empty list if no violations.
        """
        ...


class NoNegativePrices:
    """Validate that price columns do not contain negative values."""

    def __init__(self, price_columns: list[str] | None = None):
        """Initialize with optional specific price columns.

        Args:
            price_columns: List of column names to check for negative prices.
                If None, checks common price column names.
        """
        self.price_columns = price_columns or [
            'open', 'high', 'low', 'close', 'price',
            'adj_open', 'adj_high', 'adj_low', 'adj_close'
        ]

    def validate(self, df: pl.DataFrame) -> list[str]:
        """Check for negative values in price columns.

        Args:
            df: DataFrame to validate

        Returns:
            list[str]: Violation messages for negative prices found
        """
        violations = []

        for col in self.price_columns:
            if col in df.columns:
                negative_count = df.filter(pl.col(col) < 0).height
                if negative_count > 0:
                    violations.append(
                        f"Column '{col}' contains {negative_count} negative values"
                    )

        return violations


class NoFutureDates:
    """Validate that date columns do not contain future dates."""

    def __init__(self, date_columns: list[str] | None = None):
        """Initialize with optional specific date columns.

        Args:
            date_columns: List of column names to check for future dates.
                If None, checks common date column names.
        """
        self.date_columns = date_columns or [
            'date', 'timestamp', 'time', 'datetime',
            'fetched_at', 'observed_at', 'created_at'
        ]

    def validate(self, df: pl.DataFrame) -> list[str]:
        """Check for future dates in date columns.

        Args:
            df: DataFrame to validate

        Returns:
            list[str]: Violation messages for future dates found
        """
        import datetime
        from zoneinfo import ZoneInfo

        violations = []
        current_time = datetime.datetime.now(datetime.timezone.utc)

        for col in self.date_columns:
            if col in df.columns:
                # Check column dtype and handle appropriately
                col_dtype = df.schema[col]

                if col_dtype == pl.Date:
                    # For Date columns, compare dates only
                    future_count = df.filter(
                        pl.col(col) > pl.lit(current_time.date()).cast(pl.Date)
                    ).height
                elif col_dtype.base_type() == pl.Datetime:
                    raw_time_unit = getattr(col_dtype, "time_unit", "us")
                    time_unit: Literal["ns", "us", "ms"] = "us"
                    if raw_time_unit in {"ns", "us", "ms"}:
                        time_unit = cast("Literal['ns', 'us', 'ms']", raw_time_unit)
                    time_zone = getattr(col_dtype, "time_zone", None)
                    if isinstance(time_zone, str) and time_zone:
                        now_value = current_time.astimezone(ZoneInfo(time_zone))
                        cast_dtype = pl.Datetime(time_unit=time_unit, time_zone=time_zone)
                    else:
                        now_value = current_time.replace(tzinfo=None)
                        cast_dtype = pl.Datetime(time_unit=time_unit)
                    future_count = df.filter(
                        pl.col(col) > pl.lit(now_value).cast(cast_dtype)
                    ).height
                else:
                    continue

                if future_count > 0:
                    violations.append(
                        f"Column '{col}' contains {future_count} future dates"
                    )

        return violations


class NoDuplicateRows:
    """Validate that there are no duplicate rows based on specified columns."""

    def __init__(self, subset: list[str] | None = None):
        """Initialize with optional subset of columns to check for duplicates.

        Args:
            subset: List of column names to check for duplicates.
                If None, checks all columns for exact duplicates.
        """
        self.subset = subset

    def validate(self, df: pl.DataFrame) -> list[str]:
        """Check for duplicate rows.

        Args:
            df: DataFrame to validate

        Returns:
            list[str]: Violation messages for duplicate rows found
        """
        violations = []

        if self.subset:
            # Check duplicates on specific columns using groupby
            existing_subset = [column for column in self.subset if column in df.columns]
            if not existing_subset:
                duplicate_count = 0
            else:
                duplicate_result = df.group_by(existing_subset).agg(pl.len().alias("count"))\
                    .filter(pl.col("count") > 1)\
                    .select((pl.col("count") - 1).sum())
                duplicate_count = duplicate_result.item() if not duplicate_result.is_empty() else 0
        else:
            # Check exact duplicates across all columns
            duplicate_result = df.group_by(df.columns).agg(pl.len().alias("count"))\
                .filter(pl.col("count") > 1)\
                .select((pl.col("count") - 1).sum())
            duplicate_count = duplicate_result.item() if not duplicate_result.is_empty() else 0

        if duplicate_count > 0:
            column_info = f"on columns {self.subset}" if self.subset else "across all columns"
            violations.append(
                f"Found {duplicate_count} duplicate rows {column_info}"
            )

        return violations


def parse_violation_count(message: str) -> int:
    """Extract violation counts from rule messages.

    Accepted patterns are case-insensitive:
    - "contains <N>"
    - "found <N>"

    If no pattern matches, this falls back to 1 so each violation message still
    contributes at least one unit to aggregated counts.
    """
    contains_match = re.search(r"\bcontains\s+(\d+)\b", message, flags=re.IGNORECASE)
    if contains_match is not None:
        return int(contains_match.group(1))
    found_match = re.search(r"\bfound\s+(\d+)\b", message, flags=re.IGNORECASE)
    if found_match is not None:
        return int(found_match.group(1))
    return 1


async def validate_data_quality(
    *,
    table: str,
    df: pl.DataFrame,
    result: RunResult,
    result_lock: asyncio.Lock,
    logger: Any,
) -> None:
    schema = get_table_schema(table)
    if not schema or not schema.quality_rules:
        return

    violations_count = 0
    for rule in schema.quality_rules:
        try:
            violation_messages = rule.validate(df)
            if violation_messages:
                for violation_message in violation_messages:
                    violations_count += parse_violation_count(violation_message)
                logger.warning(
                    "Data quality violations in table %s: %s",
                    table,
                    ", ".join(violation_messages),
                )
        except Exception as exc:
            logger.warning(
                "Failed to execute quality rule %s for table %s: %s",
                type(rule).__name__,
                table,
                exc,
            )

    if violations_count > 0:
        async with result_lock:
            result.add_quality_violations(table=table, count=violations_count)
