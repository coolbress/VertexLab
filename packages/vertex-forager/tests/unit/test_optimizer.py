from __future__ import annotations

import polars as pl
import pytest
from vertex_forager.providers.sharadar.utils import OptimizedBulkCalculator


class TestOptimizedBulkCalculator:
    """Test suite for OptimizedBulkCalculator."""

    @pytest.fixture
    def optimizer(self) -> OptimizedBulkCalculator:
        return OptimizedBulkCalculator()

    def test_heuristic_bulk_packing_returns_chunks(
        self, optimizer: OptimizedBulkCalculator
    ) -> None:
        """Test that heuristic bulk packing returns chunks of tickers."""
        tickers = [f"T{i}" for i in range(25)]
        bulks = optimizer._heuristic_bulk_packing(tickers)

        # Expectation: 2 chunks (20 + 5)
        # ["T0,T1...T19", "T20...T24"]
        assert len(bulks) == 2
        assert len(bulks[0].split(",")) == 20
        assert len(bulks[1].split(",")) == 5

    def test_optimize_without_metadata_falls_back_to_heuristic(
        self, optimizer: OptimizedBulkCalculator
    ) -> None:
        """Test that optimize falls back to heuristic when metadata is None or empty."""
        tickers = ["AAPL", "MSFT"]

        # 1. Metadata None
        bulks_none = optimizer.optimize(tickers, None, None, None)
        assert len(bulks_none) == 1  # 2 tickers fit in 1 chunk (size 20)
        assert bulks_none[0] == "AAPL,MSFT"

        # 2. Metadata Empty
        bulks_empty = optimizer.optimize(tickers, pl.DataFrame(), None, None)
        assert len(bulks_empty) == 1
        assert bulks_empty[0] == "AAPL,MSFT"

    def test_optimize_with_missing_columns_falls_back_to_heuristic(
        self, optimizer: OptimizedBulkCalculator
    ) -> None:
        """Test that optimize falls back when metadata lacks required columns."""
        tickers = ["AAPL"]
        # Missing start/end dates
        bad_meta = pl.DataFrame({"ticker": ["AAPL"]})

        bulks = optimizer.optimize(tickers, bad_meta, None, None)
        assert bulks[0] == "AAPL"

    def test_optimize_with_valid_metadata_packs_bulks(
        self, optimizer: OptimizedBulkCalculator
    ) -> None:
        """Test that optimize correctly packs bulks with valid metadata."""
        # Setup metadata for small tickers that should be packed together
        # Assumption: estimate_rows returns small number for short date range
        tickers = [f"T{i}" for i in range(10)]

        data = {
            "ticker": tickers,
            "firstpricedate": ["2020-01-01"] * 10,
            "lastpricedate": ["2020-01-10"] * 10,  # 10 days range -> very few rows
        }
        meta_df = pl.DataFrame(data)

        # Should be packed into 1 bulk because 10 tickers * small rows < 9500 rows
        # and 10 tickers < 100 tickers limit
        bulks = optimizer.optimize(tickers, meta_df, "2020-01-01", "2020-01-10")

        assert len(bulks) == 1
        assert bulks[0] == ",".join(tickers)

    def test_optimize_splits_large_bulks(
        self, optimizer: OptimizedBulkCalculator
    ) -> None:
        """Test that optimize splits bulks when ticker count exceeds limit."""
        # 105 tickers -> should be 2 bulks (100 + 5)
        tickers = [f"T{i}" for i in range(105)]

        data = {
            "ticker": tickers,
            "firstpricedate": ["2020-01-01"] * 105,
            "lastpricedate": ["2020-01-02"] * 105,  # Minimal rows
        }
        meta_df = pl.DataFrame(data)

        bulks = optimizer.optimize(tickers, meta_df, None, None)

        assert len(bulks) == 2
        assert len(bulks[0].split(",")) == 100
        assert len(bulks[1].split(",")) == 5
