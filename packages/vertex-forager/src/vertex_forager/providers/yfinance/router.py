from __future__ import annotations

import logging
import pickle
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime, timezone
from typing import Any, Final

import pandas as pd
import polars as pl
from polars.exceptions import ComputeError
from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    HttpMethod,
    ParseResult,
    RequestSpec,
)
from vertex_forager.routers.base import BaseRouter
from vertex_forager.providers.yfinance.schema import DATASET_TABLE

logger = logging.getLogger("vertex_forager.providers.yfinance.router")


class YFinanceRouter(BaseRouter):
    """Router for Yahoo Finance datasets (via yfinance).
    
    Summary:
        - Processes all datasets per-symbol (no bulk price batching).
        - Applies conservative rate limiting to avoid IP bans.
        - Converts pickle/pandas outputs to Polars, flattens MultiIndex,
          and normalizes wide→long structures where needed.
        - Executes via YFinanceHttpExecutor for thread-safe calls.
    
    Args:
        api_key: Optional, unused (free API).
        rate_limit: Requests per minute (default 60).
        start_date: Optional start date (YYYY-MM-DD) for price dataset.
        end_date: Optional end date (YYYY-MM-DD) for price dataset.
        **kwargs: Additional configuration (e.g., price_batch_size).
    
    Attributes:
        PRICE_BATCH_SIZE: Default suggested batch size for internal heuristics.
        THREADS_THRESHOLD: Concurrency threshold for execution strategy.
    
    Implementation Notes:
        - generate_jobs yields one job per symbol across datasets.
        - Request params for price include interval/group_by and optional date filters.
        - Non-price datasets use Ticker properties; date filters are not applied.
    """

    # Constants for batching and processing
    PRICE_BATCH_SIZE: Final[int] = 250
    THREADS_THRESHOLD: Final[int] = 50
    
    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limit: int,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize YFinanceRouter.

        Args:
            api_key: Unused, kept for interface compatibility.
            rate_limit: Requests per minute.
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            **kwargs: Additional arguments.
        """
        self._api_key = None
        self._rate_limit = 60
        self._start_date = start_date
        self._end_date = end_date
        raw_bs = kwargs.get("price_batch_size", self.PRICE_BATCH_SIZE)
        try:
            bs_int = int(raw_bs)
        except (ValueError, TypeError):
            logger.debug(f"Failed to parse price_batch_size='{raw_bs}', using default={self.PRICE_BATCH_SIZE}")
            bs_int = self.PRICE_BATCH_SIZE
        self._price_batch_size = max(1, min(500, bs_int))

    @property
    def provider(self) -> str:
        """Return provider name.
        
        Returns:
            str: Provider name ('yfinance').
        """
        return "yfinance"
    
    @property
    def rate_limit(self) -> int:
        """Return rate limit.
        
        Returns:
            int: Requests per minute.
        """
        return self._rate_limit
    
    async def generate_jobs(
        self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        """Generate fetch jobs.
        
        Args:
            dataset: Target dataset name (e.g., 'price', 'financials').
            symbols: Sequence of ticker symbols, or None for unsupported bulk operations.
            **kwargs: Additional parameters forwarded to request construction.
        
        Returns:
            AsyncIterator[FetchJob]: Stream of jobs constructed per symbol.
        
        Raises:
            NotImplementedError: If bulk ticker listing is requested.
            ValueError: If required 'symbols' are missing for a dataset.
        """
        # -------- Validate Symbols --------
        
        # YFinance requires symbols for almost all datasets
        if not symbols:
            if dataset == "tickers":
                raise NotImplementedError("YFinance does not support bulk ticker listing.")
            
            raise ValueError(
            f"YFinance provider requires 'symbols' list for dataset '{dataset}'."
            )

        # -------- Build Batch Jobs --------
        
        # We intentionally avoid yfinance's multi-ticker bulk download because:
        # 1) It uses shared internal dictionaries with threaded updates that can raise
        #    RuntimeError/KeyError under concurrency (e.g., "dictionary changed size").
        # 2) Our pipeline enforces rate limit and concurrency per request; per-ticker
        #    jobs provide predictable backpressure and error isolation.
        # 3) Stability across notebook and CI environments is better with single-ticker
        #    calls; batching brings little benefit when RPM is enforced globally.
        
        # -------- Build Per-Symbol Jobs --------
        
        # Create 1 job per ticker to leverage FlowController granularly
        for symbol in symbols:
            yield self._build_single_symbol_job(symbol=symbol, dataset=dataset)

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        """Parse raw pickled payload into structured packets.
        
        Args:
            job: Fetch job that produced this payload.
            payload: Raw response bytes (pickle-serialized).
        
        Returns:
            ParseResult: Normalized packets and any next jobs.
        
        Raises:
            Exception: Unexpected errors are re-raised after logging.
        """
        try:
            # Check empty payload using BaseRouter helper
            empty_result = self._check_empty_response(payload=payload)
            if empty_result:
                return empty_result

            # -------- Deserialize Pickle --------
            
            # Raw Data from HttpExecutor
            data = pickle.loads(payload)

            # -------- Convert to Polars --------
            
            is_batch = job.context.get("is_batch", False)
            df_pl = self._convert_to_polars(data, is_batch=is_batch)

            # Check empty DataFrame using BaseRouter helper
            empty_result = self._check_empty_response(frame=df_pl)
            if empty_result:
                return empty_result

            # -------- Normalize Columns --------
            
            # Standardize column names using BaseRouter helper
            df_pl = self._normalize_columns(df_pl)

            # -------- Transform Dataset Structure --------
            
            # Apply dataset-specific transformations
            dataset = job.dataset
            observed_at = datetime.now(timezone.utc)
            if dataset == "price":
                df_pl = self._transform_price(df_pl)
            elif dataset in ("financials", "quarterly_financials", "balance_sheet", "quarterly_balance_sheet", "cashflow", "quarterly_cashflow"):
                df_pl = self._transform_financials(df_pl, dataset)
            elif dataset == "major_holders":
                df_pl = self._transform_major_holders(df_pl)
            elif dataset in ("institutional_holders", "mutualfund_holders"):
                df_pl = self._transform_holders_detailed(df_pl)
            elif dataset == "insider_roster_holders":
                df_pl = self._transform_insider_roster(df_pl)
            elif dataset == "insider_purchases":
                df_pl = self._transform_insider_purchases(df_pl)
            elif dataset == "calendar":
                df_pl = self._transform_calendar(df_pl)
            elif dataset == "news":
                df_pl = self._transform_news(df_pl)
            elif dataset in ("info", "fast_info"):
                pass
            else:
                if "index" in df_pl.columns:
                    df_pl = df_pl.drop(["index"])

            # -------- Inject Metadata --------
            
            # Inject essential columns (ticker)
            symbol = job.context.get("symbol")
            if symbol and "ticker" not in df_pl.columns:
                df_pl = df_pl.with_columns(pl.lit(symbol).alias("ticker"))
            
            # Add provider metadata
            df_pl = self._add_provider_metadata(frame=df_pl, observed_at=observed_at)

            if df_pl.is_empty():
                return ParseResult(packets=[], next_jobs=[])

            # -------- Build Frame Packet --------
            
            packet = FramePacket(
                provider=self.provider,
                table=DATASET_TABLE.get(job.dataset, f"yfinance_{job.dataset}"),
                frame=df_pl,
                observed_at=observed_at,
                context=job.context,
            )

            return ParseResult(packets=[packet], next_jobs=[])
        
        except (pickle.UnpicklingError, ValueError, TypeError):
            logger.exception("parse failed for job %s", job)
            return ParseResult(packets=[], next_jobs=[])
        except Exception:
            logger.exception("Unexpected error in parse for job %s", job)
            raise
        
    # --------------------------------------
    # Generate Jobs Helpers
    # --------------------------------------
    
    def _build_request_params(self, *, dataset: str) -> dict[str, object]:
        """Unified parameter builder for yfinance library calls."""
        params: dict[str, object] = {"dataset": dataset}
        if dataset == "price":
            params.update({
                "interval": "1d",
                "group_by": "ticker",
                "auto_adjust": False,
                "prepost": False,
                "threads": False,
                "progress": False,
            })
            if self._start_date:
                params["start"] = self._start_date
            if self._end_date:
                params["end"] = self._end_date
            if not self._start_date and not self._end_date:
                params["period"] = "max"
            elif not self._start_date and self._end_date:
                params["period"] = "max"
            params["endpoint"] = "download"
            return params
        # Default: property access on Ticker (non-price).
        # No additional request parameters are supported; date filters are not applicable.
        params["endpoint"] = "ticker"
        return params
    
    # (Batch job removed: we no longer perform bulk downloads for price.)
    
    # ------ Build per-symbol job: construct URL and spec for single ticker ------
    def _build_single_symbol_job(self, *, symbol: str, dataset: str) -> FetchJob:
        """Build a per-symbol job, applying dataset-specific options."""
        url = f"yfinance://{symbol}"
        params = self._build_request_params(dataset=dataset)
        return FetchJob(
            provider=self.provider,
            dataset=dataset,
            symbol=symbol,
            spec=RequestSpec(method=HttpMethod.GET, url=url, params=params),
            context={"symbol": symbol, "dataset": dataset},
        )

    # --------------------------------------
    # Parse Helpers 
    # --------------------------------------
    
    def _convert_to_polars(self, data: Any, is_batch: bool = False) -> pl.DataFrame:
        """Convert raw yfinance data to Polars DataFrame."""
        try:
            # Case A: Pandas DataFrame or Series
            if isinstance(data, (pd.DataFrame, pd.Series)):
                if data.empty:
                    return pl.DataFrame()
                
                # Handle Series
                if isinstance(data, pd.Series):
                    data = data.to_frame()
                
                # Handle MultiIndex Columns (e.g. yf.download with group_by='ticker')
                if isinstance(data.columns, pd.MultiIndex):
                    if is_batch:
                        # Strategy: Stack Ticker to column for Bulk Data
                        # Input: Index=Date, Cols=(Ticker, Metric)
                        # Output: Index=Row, Cols=(Date, Ticker, Metric...)
                        
                        # Check nlevels
                        if data.columns.nlevels >= 2:
                            # Stack the top level (Ticker)
                            # Note: yfinance typically returns (Ticker, Price) if group_by='ticker'
                            try:
                                data = data.stack(level=0, future_stack=True)
                            except TypeError:
                                data = data.stack(level=0)
                            
                            # If stacking resulted in Series (unlikely for OHLCV but possible), convert to Frame
                            if isinstance(data, pd.Series):
                                data = data.to_frame()
                    else:
                        # Strategy: Flatten by dropping the top level (Ticker) since we inject it later.
                        if data.columns.nlevels >= 2:
                            data.columns = data.columns.droplevel(0)
                        else:
                            data.columns = ['_'.join(map(str, col)).strip() for col in data.columns.values]

                # Handle Index (Date/Row Label)
                data = data.reset_index()
                
                # Convert to Polars
                try:
                    return pl.from_pandas(data)
                except (ValueError, TypeError, ComputeError):
                    return pl.from_pandas(pd.DataFrame(data.to_dict()))

            # Case B: Dictionary (e.g. info)
            elif isinstance(data, dict):
                # Ensure values are supported types (str, int, float, bool, None, list, dict, date, datetime)
                # Note: yfinance calendar returns dates in lists or as objects
                allowed_types = (str, int, float, bool, type(None), list, dict, date, datetime)
                clean_data = {k: v for k, v in data.items() if isinstance(v, allowed_types)}
                return pl.DataFrame([clean_data])
            
            # Case C: List
            elif isinstance(data, list):
                return pl.DataFrame(data)

            # Case D: Object with to_dict
            elif hasattr(data, "to_dict"):
                return pl.DataFrame([data.to_dict()])
            
            # Default
            return pl.DataFrame([data] if data else [])

        except (ValueError, TypeError, ComputeError) as e:
            logger.warning(f"Failed to convert data to Polars: {e}")
            return pl.DataFrame()
        except Exception:
            logger.exception("Unexpected failure converting data to Polars")
            raise
    
    def _transform_price(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "date" not in frame.columns:
            if "index" in frame.columns:
                frame = frame.rename({"index": "date"})
            elif "level_0" in frame.columns:
                frame = frame.rename({"level_0": "date"})
        return frame
    
    def _transform_financials(self, frame: pl.DataFrame, dataset: str) -> pl.DataFrame:
        if "index" in frame.columns:
            frame = frame.rename({"index": "metric"})
        elif "breakdown" in frame.columns:
            frame = frame.rename({"breakdown": "metric"})
        id_vars = ["metric"]
        if "ticker" in frame.columns:
            id_vars.append("ticker")
        value_vars = [c for c in frame.columns if c not in id_vars]
        if value_vars:
            frame = frame.melt(id_vars=id_vars, value_vars=value_vars, variable_name="date", value_name="value")
            if "date" in frame.columns:
                date_str = pl.col("date").cast(pl.Utf8, strict=False)
                frame = frame.with_columns(date_str.str.replace(r"[_T\\s].*$", "", literal=False).alias("date"))
        period = "quarterly" if dataset.startswith("quarterly") else "annual"
        if "period" not in frame.columns:
            frame = frame.with_columns(pl.lit(period).alias("period"))
        return frame
    
    def _transform_major_holders(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "index" in frame.columns:
            frame = frame.rename({"index": "metric"})
        cols = [c for c in frame.columns if c not in ("ticker",)]
        if "metric" in cols:
            value_cols = [c for c in cols if c != "metric"]
            if value_cols:
                val = value_cols[0]
                df = frame.select(["metric", val]).rename({val: "value"})
                df = df.with_columns(pl.lit(0).alias("_row"))
                df = df.pivot(index="_row", columns="metric", values="value").drop("_row")
                if "ticker" in frame.columns and "ticker" not in df.columns:
                    df = df.with_columns(pl.lit(frame["ticker"][0]).alias("ticker"))
                return df
        return frame
    
    def _transform_holders_detailed(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "percentage_of_shares_out" in frame.columns and "percentage_out" not in frame.columns:
            frame = frame.rename({"percentage_of_shares_out": "percentage_out"})
        if "index" in frame.columns:
            frame = frame.drop(["index"])
        return frame
    
    def _transform_insider_roster(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "index" in frame.columns:
            frame = frame.drop(["index"])
        return frame
    
    def _transform_insider_purchases(self, frame: pl.DataFrame) -> pl.DataFrame:
        drops = []
        if "index" in frame.columns:
            drops.append("index")
        if "date" in frame.columns:
            drops.append("date")
        if drops:
            frame = frame.drop(drops)
        return frame
    
    def _transform_calendar(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "earnings_date" in frame.columns:
            dt = frame.schema.get("earnings_date")
            if dt is not None and dt != pl.Date:
                frame = frame.with_columns(pl.col("earnings_date").list.first().alias("earnings_date"))
        return frame

    def _transform_news(self, frame: pl.DataFrame) -> pl.DataFrame:
        cols = []
        if "content" in frame.columns:
            cols.extend([
                pl.col("content").struct.field("title").alias("title"),
                pl.col("content").struct.field("provider").struct.field("displayName").alias("publisher"),
                pl.col("content").struct.field("contentType").alias("type"),
                pl.coalesce(
                    pl.col("content").struct.field("canonicalUrl").struct.field("url"),
                    pl.col("content").struct.field("clickThroughUrl").struct.field("url"),
                    pl.col("content").struct.field("previewUrl"),
                ).alias("link"),
                pl.col("content")
                .struct.field("pubDate")
                .str.strptime(pl.Datetime, format="%+", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("published_at"),
            ])
            frame = frame.with_columns(cols)
        keep = [c for c in ["id", "title", "publisher", "type", "link", "published_at"] if c in frame.columns]
        others = [c for c in frame.columns if c not in keep + ["content"]]
        frame = frame.select(others + keep)
        return frame
