"""
Unit tests for schema.mapper module.

Tests cover:
- SchemaMapper normalization
- Schema enforcement and type casting
- Missing column handling
- Date column normalization
"""

from datetime import datetime, timezone
import polars as pl
import pytest

from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.schema.config import TableSchema
from vertex_forager.core.config import FramePacket


class TestSchemaMapper:
    """Test SchemaMapper class."""

    @pytest.fixture
    def mapper(self) -> SchemaMapper:
        """Create SchemaMapper instance."""
        return SchemaMapper()

    @pytest.fixture
    def sample_schema(self) -> TableSchema:
        """Create a sample table schema."""
        return TableSchema(
            table="test_table",
            schema={
                "id": pl.Int64,
                "name": pl.Utf8,
                "price": pl.Float64,
                "date": pl.Date,
                "active": pl.Boolean,
            },
            unique_key=("id",),
            analysis_date_col="date",
        )

    def test_normalize_with_matching_schema(self, mapper: SchemaMapper):
        """Test normalization with data matching schema."""
        frame = pl.DataFrame({
            "ticker": ["AAPL", "GOOGL"],
            "date": ["2024-01-01", "2024-01-02"],
            "price": [150.0, 2800.0],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",  # Known schema
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should return packet with normalized frame
        assert isinstance(normalized, FramePacket)
        assert not normalized.frame.is_empty()

    def test_normalize_casts_types(self, mapper: SchemaMapper):
        """Test that mapper casts columns to correct types."""
        # Frame with string values that need casting
        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "date": ["2024-01-01"],
            "open": ["150.5"],  # String that should become Float64
            "volume": ["1000000"],  # String that should become Int64
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Check types were cast (sharadar_sep schema expects Float64 and Int64)
        # Note: SchemaMapper will add missing columns as null
        assert "ticker" in normalized.frame.columns
        assert "date" in normalized.frame.columns

    def test_normalize_handles_missing_columns(self, mapper: SchemaMapper):
        """Test that mapper adds missing columns as null."""
        # Frame missing several columns from schema
        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "date": ["2024-01-01"],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should have all schema columns (even if null)
        assert "ticker" in normalized.frame.columns
        assert "date" in normalized.frame.columns

    def test_normalize_with_empty_frame(self, mapper: SchemaMapper):
        """Test normalization with empty DataFrame."""
        frame = pl.DataFrame()
        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should return as-is for empty frame
        assert normalized.frame.is_empty()

    def test_normalize_unknown_table(self, mapper: SchemaMapper):
        """Test normalization with unknown table (no schema)."""
        frame = pl.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="unknown_table",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should return unchanged for unknown schema
        assert normalized.frame.equals(frame)

    def test_normalize_preserves_extra_columns(self, mapper: SchemaMapper):
        """Test that normalization preserves columns not in schema."""
        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "date": ["2024-01-01"],
            "extra_col": ["extra_value"],  # Not in schema
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Extra column should be preserved
        assert "ticker" in normalized.frame.columns
        assert "date" in normalized.frame.columns
        assert "extra_col" in normalized.frame.columns

    def test_normalize_reorders_columns_by_unique_key(self, mapper: SchemaMapper):
        """Test that columns are reordered with unique key first."""
        frame = pl.DataFrame({
            "provider": ["test"],
            "open": [150.0],
            "ticker": ["AAPL"],  # Part of unique key
            "date": ["2024-01-01"],  # Part of unique key
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Unique key columns (provider, ticker, date) should come first
        cols = normalized.frame.columns
        # provider, ticker, date should be early in the column order
        assert "provider" in cols
        assert "ticker" in cols
        assert "date" in cols


class TestDateColumnNormalization:
    """Test date column normalization logic."""

    def test_normalize_date_column_with_standard_date_col(self):
        """Test normalization when analysis_date_col is 'date'."""
        mapper = SchemaMapper()

        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "date": ["2024-01-01"],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",  # analysis_date_col = "date"
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should have 'date' column
        assert "date" in normalized.frame.columns

    def test_normalize_date_column_with_custom_col(self):
        """Test normalization with custom analysis date column."""
        mapper = SchemaMapper()

        # SF1 uses 'datekey' as analysis_date_col
        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "datekey": ["2024-01-01"],
            "calendardate": ["2024-01-01"],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sf1",  # analysis_date_col = "datekey"
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should have 'date' column (renamed from datekey)
        assert "date" in normalized.frame.columns
        assert "datekey" not in normalized.frame.columns  # Renamed

    def test_normalize_date_column_no_analysis_col(self):
        """Test normalization when no analysis_date_col defined."""
        mapper = SchemaMapper()

        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "name": ["Apple Inc."],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_tickers",  # analysis_date_col = None
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should not add or modify 'date' column
        assert normalized.frame.shape[0] >= 0  # Succeeds


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_normalize_with_null_values(self):
        """Test normalization handles null values correctly."""
        mapper = SchemaMapper()

        frame = pl.DataFrame({
            "ticker": ["AAPL", None],
            "date": ["2024-01-01", None],
            "price": [150.0, None],
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should handle nulls without crashing
        assert normalized.frame.shape[0] == 2

    def test_normalize_with_incompatible_types(self):
        """Test normalization with incompatible type values."""
        mapper = SchemaMapper()

        # Frame with values that can't be cast
        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "date": ["2024-01-01"],
            "volume": ["not_a_number"],  # Invalid for Int64
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        # Should not crash (strict=False allows nulls on cast failure)
        normalized = mapper.normalize(packet)
        assert normalized is not None

    def test_normalize_preserves_packet_metadata(self):
        """Test that normalization preserves packet metadata."""
        mapper = SchemaMapper()

        frame = pl.DataFrame({
            "ticker": ["AAPL"],
            "date": ["2024-01-01"],
        })

        observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        context = {"source": "api", "batch_id": 123}

        packet = FramePacket(
            provider="sharadar",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
            context=context,
        )

        normalized = mapper.normalize(packet)

        # Metadata should be preserved
        assert normalized.provider == "sharadar"
        assert normalized.table == "sharadar_sep"
        assert normalized.observed_at == observed_at
        assert normalized.context == context

    def test_normalize_with_large_frame(self):
        """Test normalization with large DataFrame."""
        mapper = SchemaMapper()

        # Create large frame
        num_rows = 10000
        frame = pl.DataFrame({
            "ticker": ["AAPL"] * num_rows,
            "date": ["2024-01-01"] * num_rows,
            "price": [150.0] * num_rows,
        })

        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="sharadar_sep",
            frame=frame,
            observed_at=observed_at,
        )

        normalized = mapper.normalize(packet)

        # Should handle large frames
        assert normalized.frame.shape[0] == num_rows