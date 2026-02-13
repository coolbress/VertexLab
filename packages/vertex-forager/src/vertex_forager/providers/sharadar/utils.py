from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Final

import polars as pl

from vertex_forager.schema.config import TableSchema
from vertex_forager.providers.sharadar.schema import (
    SHARADAR_ACTIONS,
    SHARADAR_DAILY,
    SHARADAR_SEP,
    SHARADAR_SF1,
    SHARADAR_SF2,
    SHARADAR_SF3,
    SHARADAR_SP500,
    SHARADAR_TICKERS,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Dataset Configuration (Moved from router.py)
# --------------------------------------------------------------------------

DATASET_ENDPOINT: Final[dict[str, str]] = {
    "price": "SEP",
    "fundamental": "SF1",
    "daily": "DAILY",
    "tickers": "TICKERS",
    "actions": "ACTIONS",
    "insider": "SF2",
    "institutional": "SF3",
    "sp500": "SP500",
}

DATASET_TABLE: Final[dict[str, str]] = {
    "price": "sharadar_sep",
    "tickers": "sharadar_tickers",
    "fundamental": "sharadar_sf1",
    "daily": "sharadar_daily",
    "actions": "sharadar_actions",
    "insider": "sharadar_sf2",
    "institutional": "sharadar_sf3",
    "sp500": "sharadar_sp500",
}

DATASET_SCHEMA: Final[dict[str, TableSchema]] = {
    "price": SHARADAR_SEP,
    "tickers": SHARADAR_TICKERS,
    "fundamental": SHARADAR_SF1,
    "insider": SHARADAR_SF2,
    "institutional": SHARADAR_SF3,
    "actions": SHARADAR_ACTIONS,
    "daily": SHARADAR_DAILY,
    "sp500": SHARADAR_SP500,
}

DATE_FILTER_COL: Final[dict[str, str]] = {
    "price": "date",
    "fundamental": "calendardate",
    "daily": "date",
    "actions": "date",
    "insider": "filingdate",
    "institutional": "calendardate",
    "sp500": "date",
}

INTERNAL_COLS: Final[set[str]] = {"provider", "fetched_at"}


# --------------------------------------------------------------------------
# Bulk Optimization
# --------------------------------------------------------------------------


class OptimizedBulkCalculator:
    """Optimizes ticker batches for Sharadar API to maximize density and minimize pagination."""

    def optimize(
        self,
        tickers: list[str],
        metadata: pl.DataFrame | None,
        start_date: str | None,
        end_date: str | None,
        table_name: str = "sharadar_sep",
    ) -> list[str]:
        """Optimize ticker batches using metadata to maximize density."""
        if metadata is None or metadata.is_empty():
            logger.warning(
                "No metadata provided. Falling back to heuristic bulk packing."
            )
            return self._heuristic_bulk_packing(tickers)

        # 1. Determine columns based on table
        # Classify dataset density:
        # - Low Density (Quarterly/Sparse): SF1 (Fundamental), SF3 (Institutional), SF2 (Insider), ACTIONS
        # - High Density (Daily): SEP (Price), DAILY (Metrics), SP500
        # Note: SF2/ACTIONS are event-based but generally much sparser than daily price data, so we treat them as low density to pack more tickers.

        table_lower = table_name.lower()
        is_low_density = any(x in table_lower for x in ("sf1", "sf3", "sf2", "actions"))

        start_col = "firstquarter" if is_low_density else "firstpricedate"
        end_col = "lastquarter" if is_low_density else "lastpricedate"

        # 2. Build Metadata Map
        meta_map: dict[str, tuple[date, date]] = {}
        required_cols = {"ticker", start_col, end_col}

        if not required_cols.issubset(set(metadata.columns)):
            logger.warning(
                f"Metadata missing required columns {required_cols - set(metadata.columns)}. Falling back to heuristic."
            )
            return self._heuristic_bulk_packing(tickers)

        for row in metadata.iter_rows(named=True):
            t = row.get("ticker")
            s = row.get(start_col)
            e = row.get(end_col)

            valid_t = isinstance(t, str)
            valid_s = isinstance(s, (str, date, datetime))
            valid_e = isinstance(e, (str, date, datetime))

            if valid_t and valid_s and valid_e:
                try:
                    if isinstance(s, str):
                        s = datetime.strptime(s, "%Y-%m-%d").date()
                    elif isinstance(s, datetime):
                        s = s.date()

                    if isinstance(e, str):
                        e = datetime.strptime(e, "%Y-%m-%d").date()
                    elif isinstance(e, datetime):
                        e = e.date()

                    meta_map[t] = (s, e)
                except ValueError:
                    logger.warning(
                        f"Failed to parse date for ticker {t}: start={s}, end={e}. Skipping."
                    )
                    continue
            else:
                logger.debug(
                    f"Skipping metadata for {t}: {start_col}={s}, {end_col}={e}"
                )

        # 3. Bin Packing
        bulks: list[str] = []
        current_bulk: list[str] = []
        current_rows = 0
        MAX_ROWS = 9500  # Safety margin for 10k limit
        MAX_TICKERS_PER_BULK = (
            100  # Safety margin for URL length (Sharadar recommendation)
        )

        try:
            req_start = (
                datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
            )
            req_end = (
                datetime.strptime(end_date, "%Y-%m-%d").date()
                if end_date
                else date.today()
            )
        except ValueError as e:
            logger.warning(
                f"Invalid date format in request: {e}. Defaulting to full range."
            )
            req_start = None
            req_end = date.today()

        for t in tickers:
            # Estimate rows
            if t in meta_map:
                meta_start, meta_end = meta_map[t]

                # Effective range
                eff_start = max(req_start, meta_start) if req_start else meta_start
                eff_end = min(req_end, meta_end)

                if eff_start > eff_end:
                    est = 0
                else:
                    delta_days = (eff_end - eff_start).days
                    if is_low_density:
                        # Approx 4 rows per year -> delta_days / 90
                        est = int(delta_days / 90) + 1
                    else:
                        # Trading days approx 0.69
                        est = int(delta_days * 0.69)

                    est = max(1, est)

                # DEBUG LOG
                logger.debug(f"Ticker {t}: {meta_start}~{meta_end} -> Est Rows: {est}")
            else:
                est = self.estimate_rows(start_date, end_date, table_name)
                logger.debug(f"Ticker {t}: No Metadata -> Fallback Est Rows: {est}")

            # Packing Logic
            # Condition 1: Single ticker exceeds row limit -> Force single request
            if est >= MAX_ROWS:
                if current_bulk:
                    bulks.append(",".join(current_bulk))
                    current_bulk = []
                    current_rows = 0
                bulks.append(t)
                continue

            # Condition 2: Batch full (Rows or Ticker Count)
            if (current_rows + est > MAX_ROWS) or (
                len(current_bulk) >= MAX_TICKERS_PER_BULK
            ):
                bulks.append(",".join(current_bulk))
                current_bulk = [t]
                current_rows = est
            else:
                current_bulk.append(t)
                current_rows += est

        if current_bulk:
            bulks.append(",".join(current_bulk))

        return bulks

    def _heuristic_bulk_packing(self, tickers: list[str]) -> list[str]:
        """Simple chunking when no metadata is available."""
        # Conservative chunk size
        CHUNK_SIZE = 20
        bulks = []
        for i in range(0, len(tickers), CHUNK_SIZE):
            chunk = tickers[i : i + CHUNK_SIZE]
            bulks.append(",".join(chunk))
        return bulks

    def estimate_rows(
        self,
        start_date: str | None,
        end_date: str | None,
        table_name: str = "sharadar_sep",
    ) -> int:
        """Estimate the number of rows per ticker based on date range and table type."""
        table_lower = table_name.lower()
        is_low_density = any(x in table_lower for x in ("sf1", "sf3", "sf2", "actions"))

        if start_date is None:
            # Full history assumption (e.g. 1998 ~ now) -> approx 25+ years
            if is_low_density:
                # 25 * 4 = 100 rows
                return 150
            else:
                # 25 * 252 = 6300 rows
                # Some tickers might be longer, so conservative estimate is high
                return 15000

        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = (
                datetime.strptime(end_date, "%Y-%m-%d").date()
                if end_date
                else date.today()
            )

            delta_days = (end - start).days
            if delta_days < 0:
                return 15000 if not is_low_density else 150

            if is_low_density:
                # Approx 4 rows per year
                estimated_rows = int(delta_days / 90) + 1
            else:
                # Trading days approx 5/7 = ~0.71, minus holidays ~ 0.69
                estimated_rows = int(delta_days * 0.69)

            return max(1, estimated_rows)
        except ValueError:
            return 15000 if not is_low_density else 150
