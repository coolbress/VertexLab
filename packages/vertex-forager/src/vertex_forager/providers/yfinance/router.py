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
    ParseResult,
)
from vertex_forager.routers.base import BaseRouter
from vertex_forager.providers.yfinance.schema import DATASET_TABLE, DATASET_ENDPOINT
from vertex_forager.core.types import JSONValue
from vertex_forager.core.types import YFinanceDataset
from vertex_forager.routers.jobs import single_symbol_job, build_symbol_context
from vertex_forager.routers.errors import raise_yfinance_parse_error

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
    flexible_schema: bool = True

    # Constants for batching and processing
    PRICE_BATCH_SIZE: Final[int] = 250
    THREADS_THRESHOLD: Final[int] = 50
    
    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limit: int = 60,
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
        if isinstance(rate_limit, int) and rate_limit > 0:
            self._rate_limit = rate_limit
        else:
            logger.warning("Invalid rate_limit value '%s'; falling back to 60 rpm.", rate_limit)
            self._rate_limit = 60
        self._start_date = start_date
        self._end_date = end_date
        raw_bs = kwargs.get("price_batch_size", self.PRICE_BATCH_SIZE)
        try:
            bs_int = int(raw_bs)
        except (ValueError, TypeError):
            logger.debug("Failed to parse price_batch_size='%s', using default=%d", raw_bs, self.PRICE_BATCH_SIZE)
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
        self, *, dataset: YFinanceDataset, symbols: Sequence[str] | None, **kwargs: object
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
        
        # Normalize symbols first, then validate
        cleaned: list[str] = []
        for symbol in symbols:
            if isinstance(symbol, str):
                clean = symbol.strip()
                if clean and any(ch.isalnum() for ch in clean):
                    cleaned.append(clean)
        if not cleaned:
            raise ValueError("YFinanceRouter: no valid symbols provided")
        # Order-preserving deduplication
        seen: set[str] = set()
        unique_cleaned: list[str] = []
        for s in cleaned:
            if s not in seen:
                seen.add(s)
                unique_cleaned.append(s)
        for clean in unique_cleaned:
            yield self._build_single_symbol_job(symbol=clean, dataset=dataset)

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
            
            # SECURITY WARNING: pickle.loads can execute arbitrary code; only use with trusted sources.
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
            elif dataset in ("financials", "quarterly_financials", "balance_sheet", "quarterly_balance_sheet", "cashflow", "quarterly_cashflow", "income_stmt", "earnings", "quarterly_earnings"):
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
        
        except (pickle.UnpicklingError, ValueError, TypeError) as e:
            logger.exception("parse failed for job %s", job)
            raise_yfinance_parse_error(e, dataset=job.dataset)
        except Exception as e:
            logger.exception("Unexpected error in parse for job %s", job)
            raise_yfinance_parse_error(e, dataset=job.dataset)
        
    # --------------------------------------
    # Generate Jobs Helpers
    # --------------------------------------
    
    def _build_request_params(self, *, dataset: YFinanceDataset) -> dict[str, JSONValue]:
        """Unified parameter builder for yfinance library calls.
        
        Returns a dict that includes a 'lib' key describing the exact library
        call the HttpExecutor should perform. This keeps HttpExecutor generic.
        """
        params: dict[str, JSONValue] = {"dataset": dataset}
        mapped = DATASET_ENDPOINT.get(dataset, dataset)
        if dataset == "price":
            kwargs: dict[str, JSONValue] = {
                "interval": "1d",
                "auto_adjust": False,
                "prepost": False,
            }
            if self._start_date:
                kwargs["start"] = self._start_date
            if self._end_date:
                kwargs["end"] = self._end_date
            if not self._start_date:
                kwargs["period"] = "max"
            # Single-ticker history call is preferred over download for stability
            params["lib"] = {"type": "ticker_attr", "attr": mapped, "kwargs": kwargs}
            return params
        # Default: property access on Ticker (non-price).
        params["lib"] = {"type": "ticker_attr", "attr": mapped, "kwargs": {}}
        return params
    
    # (Batch job removed: we no longer perform bulk downloads for price.)
    
    # ------ Build per-symbol job: construct URL and spec for single ticker ------
    def _build_single_symbol_job(self, *, symbol: str, dataset: YFinanceDataset) -> FetchJob:
        """Build a per-symbol job, applying dataset-specific options."""
        url = f"yfinance://{symbol}"
        params = self._build_request_params(dataset=dataset)
        return single_symbol_job(
            provider=self.provider,
            dataset=dataset,
            symbol=symbol,
            url=url,
            params=params,
            auth=None,
            context=build_symbol_context(dataset=dataset, symbol=symbol),
        )

    # --------------------------------------
    # Parse Helpers 
    # --------------------------------------
    
    def _normalize_multiindex(self, data: pd.DataFrame, is_batch: bool) -> pd.DataFrame:
        if isinstance(data.columns, pd.MultiIndex):
            if is_batch:
                if data.columns.nlevels >= 2:
                    try:
                        data = data.stack(level=0, future_stack=True)
                    except TypeError:
                        data = data.stack(level=0)
                    if isinstance(data, pd.Series):
                        data = data.to_frame()
            else:
                if data.columns.nlevels >= 2:
                    data.columns = data.columns.droplevel(0)
                else:
                    data.columns = ['_'.join(map(str, col)).strip() for col in data.columns.values]
        return data

    def _from_pandas(self, data: pd.DataFrame | pd.Series, is_batch: bool) -> pl.DataFrame:
        if isinstance(data, pd.Series):
            data = data.to_frame()
        if hasattr(data, "empty") and data.empty:
            return pl.DataFrame()
        data = self._normalize_multiindex(data, is_batch)
        data = data.reset_index()
        try:
            return pl.from_pandas(data)
        except (ValueError, TypeError, ComputeError):
            return pl.from_pandas(pd.DataFrame(data.to_dict()))

    def _from_dict(self, data: dict) -> pl.DataFrame:
        allowed_types = (str, int, float, bool, type(None), list, dict, date, datetime)
        clean_data = {k: v for k, v in data.items() if isinstance(v, allowed_types)}
        return pl.DataFrame([clean_data])

    def _from_list(self, data: list) -> pl.DataFrame:
        return pl.DataFrame(data)

    def _from_object_with_to_dict(self, data: Any) -> pl.DataFrame:
        return pl.DataFrame([data.to_dict()])

    def _convert_to_polars(self, data: Any, is_batch: bool = False) -> pl.DataFrame:
        try:
            if isinstance(data, (pd.DataFrame, pd.Series)):
                return self._from_pandas(data, is_batch)
            if isinstance(data, dict):
                return self._from_dict(data)
            if isinstance(data, list):
                return self._from_list(data)
            if hasattr(data, "to_dict"):
                return self._from_object_with_to_dict(data)
            if data is None:
                return pl.DataFrame([])
            return pl.DataFrame([data])
        except (ValueError, TypeError, ComputeError) as e:
            logger.error("Failed to convert data to Polars: %s", e)
            raise
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
                frame = frame.with_columns(date_str.str.replace(r"[T\s_].*$", "", literal=False).alias("date"))
                # Normalize pure year values to first day of the year
                normalized = pl.when(pl.col("date").str.contains(r"^\d{4}$", literal=False)).then(
                    pl.concat_str([pl.col("date"), pl.lit("-01-01")])
                ).otherwise(pl.col("date")).alias("date")
                frame = frame.with_columns(normalized)
                # Keep only date-like strings (YYYY or YYYY-MM-DD)
                frame = frame.filter(pl.col("date").str.contains(r"^\d{4}(-\d{2}-\d{2})?$", literal=False))
                # Ensure numeric type for value
                if "value" in frame.columns:
                    frame = frame.with_columns(pl.col("value").cast(pl.Float64, strict=False).alias("value"))
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
                rename_map = {
                    "Insiders Percent Held": "insiders_percent_held",
                    "Institutions Count": "institutions_count",
                    "Institutions Float Percent Held": "institutions_float_percent_held",
                    "Institutions Percent Held": "institutions_percent_held",
                }
                present = {k: v for k, v in rename_map.items() if k in df.columns}
                if present:
                    df = df.rename(present)
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
        # Normalize column names to schema
        rename_map = {
            # original labels (pre-normalization)
            "Holder": "holder",
            "Shares": "shares",
            "Trans": "trans",
            "Insider Purchases (Last 6 months)": "insider_purchases_last_6m",
            # normalized labels (post _normalize_columns)
            "holder": "holder",
            "shares": "shares",
            "trans": "trans",
            "insider_purchases_last_6_months": "insider_purchases_last_6m",
        }
        present = {k: v for k, v in rename_map.items() if k in frame.columns}
        if present:
            frame = frame.rename(present)
        # Ensure PK fields exist and are non-null; if core field missing, drop rows
        if "insider_purchases_last_6m" in frame.columns:
            frame = frame.with_columns(pl.col("insider_purchases_last_6m").cast(pl.Utf8, strict=False).alias("insider_purchases_last_6m"))
            frame = frame.filter(pl.col("insider_purchases_last_6m").is_not_null())
        else:
            # No meaningful data -> empty
            return frame.filter(pl.lit(False))
        # Optional: make holder non-null even if not used in PK
        if "holder" in frame.columns:
            frame = frame.with_columns(pl.col("holder").cast(pl.Utf8, strict=False).fill_null("").alias("holder"))
        else:
            frame = frame.with_columns(pl.lit("").alias("holder"))
        return frame
    
    def _transform_calendar(self, frame: pl.DataFrame) -> pl.DataFrame:
        if "earnings_date" in frame.columns:
            dt = frame.schema.get("earnings_date")
            if dt is not None and isinstance(dt, pl.List):
                frame = frame.with_columns(pl.col("earnings_date").list.first().alias("earnings_date"))
        return frame

    def _transform_news(self, frame: pl.DataFrame) -> pl.DataFrame:
        def _extract_from_candidates(x: Any, candidates: list[list[str]]) -> Any:
            for path in candidates:
                cur = x
                ok = True
                for key in path:
                    if not isinstance(cur, dict):
                        ok = False
                        break
                    cur = cur.get(key)
                    if cur is None:
                        ok = False
                        break
                if ok:
                    return cur
            return None
        def _parse_dt(x: Any) -> Any:
            s = _extract_from_candidates(x, [["pubDate"]])
            if isinstance(s, str):
                try:
                    if s.endswith("Z"):
                        s = s.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except (ValueError, AttributeError):
                    return None
            return None
        cols = []
        if "content" in frame.columns:
            cols.extend([
                pl.col("content").map_elements(lambda x: _extract_from_candidates(x, [["title"]]), return_dtype=pl.Utf8).alias("title"),
                pl.col("content").map_elements(lambda x: _extract_from_candidates(x, [["provider","displayName"], ["publisher"]]), return_dtype=pl.Utf8).alias("publisher"),
                pl.col("content").map_elements(lambda x: _extract_from_candidates(x, [["contentType"]]), return_dtype=pl.Utf8).alias("type"),
                pl.col("content").map_elements(lambda x: _extract_from_candidates(x, [["canonicalUrl","url"], ["clickThroughUrl","url"], ["previewUrl"]]), return_dtype=pl.Utf8).alias("link"),
                pl.col("content").map_elements(lambda x: _parse_dt(x), return_dtype=pl.Datetime(time_zone="UTC")).alias("published_at"),
            ])
            frame = frame.with_columns(cols)
        keep = [c for c in ["id", "title", "publisher", "type", "link", "published_at"] if c in frame.columns]
        others = [c for c in frame.columns if c not in keep + ["content"]]
        frame = frame.select(others + keep)
        return frame
