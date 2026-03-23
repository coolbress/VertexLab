"""Data quality validation rules and utilities."""

from typing import Protocol

import polars as pl


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
                elif col_dtype == pl.Datetime:
                    # For Datetime columns, use timezone-aware comparison
                    future_count = df.filter(
                        pl.col(col) > pl.lit(current_time).cast(pl.Datetime)
                    ).height
                else:
                    # For other types, use naive comparison (fallback)
                    future_count = df.filter(
                        pl.col(col) > pl.lit(current_time)
                    ).height

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
            duplicate_result = df.group_by(self.subset).agg(pl.count())\
                .filter(pl.col("count") > 1)\
                .select(pl.col("count").sum())
            duplicate_count = duplicate_result.item() if not duplicate_result.is_empty() else 0
        else:
            # Check exact duplicates across all columns
            duplicate_result = df.group_by(df.columns).agg(pl.count())\
                .filter(pl.col("count") > 1)\
                .select(pl.col("count").sum())
            duplicate_count = duplicate_result.item() if not duplicate_result.is_empty() else 0

        if duplicate_count > 0:
            column_info = f"on columns {self.subset}" if self.subset else "across all columns"
            violations.append(
                f"Found {duplicate_count} duplicate rows {column_info}"
            )

        return violations
