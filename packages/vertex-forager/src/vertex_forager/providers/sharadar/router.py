from __future__ import annotations

import logging
from typing import Final
import json
import io
from collections.abc import AsyncIterator, Iterator, Sequence
import copy
from datetime import date, datetime, timezone

import polars as pl

from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    HttpMethod,
    ParseResult,
    RequestAuth,
    RequestSpec,
)
from vertex_forager.routers.base import BaseRouter
from polars.exceptions import PolarsError
from vertex_forager.providers.sharadar.schema import (
    DATASET_SCHEMA,
    DATASET_TABLE,
    DATASET_ENDPOINT,
    DATE_FILTER_COL,
    INTERNAL_COLS,
)


logger = logging.getLogger("vertex_forager.providers.sharadar.router")


class SharadarRouter(BaseRouter):
    """Data Router implementation for Sharadar (Nasdaq Data Link).

    **Request Characteristics:**
    
    1. **Smart Batching**: Uses ticker metadata cache (first/last trading dates) to efficiently pack multiple tickers within 10,000-row API limit
    2. **Cursor-based Pagination**: Uses `next_cursor_id` for complete large dataset collection
    
    **Response Characteristics:**
    
    1. **JSON Format**: Standard Nasdaq Data Link API responses
    2. **Row-based Data**: Ticker-specific time series data
    3. **Efficient Conversion**: Polars native JSON parsing with GIL release
    
    **Implementation Status:**
    
    - ✅ Smart batching with metadata cache utilization
    - ✅ Polars native JSON parsing applied
    - ✅ Cursor-based pagination auto-handling
    - ✅ Structure transformation only, type casting delegated to SchemaMapper
    """

    _BASE_URL: Final[str] = "https://data.nasdaq.com/api/v3/datatables/SHARADAR"

    _BULK_DATASETS = {
        "price",
        "fundamental",
        "daily",
        "tickers",
        "actions",
        "insider",
        "institutional",
        "sp500",
    }

    _PAGINATION_CONTEXT = {
        "pagination": {
            "cursor_param": "qopts.cursor_id",
            "meta_key": "next_cursor_id",
            "max_pages": 1000,
        }
    }

    # Constants for batch calculation and API limits
    MAX_ROWS_PER_REQUEST: Final[int] = 10000
    DEFAULT_BATCH_SIZE: Final[int] = 100
    MIN_BATCH_SIZE: Final[int] = 1
    TRADING_DAYS_RATIO: Final[float] = 0.7  # ~252 trading days per year (252/365)
    QUARTERLY_DAYS_RATIO: Final[float] = 1/90  # ~4 quarters per year

    def __init__(
        self,
        *,
        api_key: str,
        rate_limit: int,
        start_date: str | None = None,
        end_date: str | None = None,
        ticker_metadata: pl.DataFrame | None = None,
        **kwargs: object,
    ) -> None:
        """Initialize SharadarRouter.

        Args:
            api_key: Valid API key for the provider.
            rate_limit: Rate limit (requests per minute).
            start_date: Optional start date filter (YYYY-MM-DD).
            end_date: Optional end date filter (YYYY-MM-DD).
            ticker_metadata: Cached metadata DataFrame (for smart batching).
            **kwargs: Additional configuration parameters.
        """
        self._api_key = api_key
        self._rate_limit = rate_limit
        self._start_date = start_date
        self._end_date = end_date
        
        # Process metadata into efficient lookup if available
        self._ticker_ranges: dict[str, tuple[date, date]] | None = None
        if ticker_metadata is not None and not ticker_metadata.is_empty():
            self._process_ticker_metadata(ticker_metadata)

    def _process_ticker_metadata(self, df: pl.DataFrame) -> None:
        """Convert metadata DataFrame to efficient lookup dict."""
        try:
            # Check required columns
            required = {"ticker", "firstpricedate", "lastpricedate"}
            if not required.issubset(df.columns):
                logger.warning("Metadata missing required columns %s. Smart batching disabled.", required)
                return

            # Normalize date types for reliable comparison
            normalized = df.with_columns(
                [
                    pl.col("firstpricedate")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, strict=False)
                    .alias("firstpricedate"),
                    pl.col("lastpricedate")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Date, strict=False)
                    .alias("lastpricedate"),
                ]
            )

            # Filter only valid dates and build lookup
            valid_df = normalized.filter(
                pl.col("firstpricedate").is_not_null() & 
                pl.col("lastpricedate").is_not_null()
            )
            
            tickers = valid_df.get_column("ticker").to_list()
            starts = valid_df.get_column("firstpricedate").to_list()
            ends = valid_df.get_column("lastpricedate").to_list()
            self._ticker_ranges = {t: (s, e) for t, s, e in zip(tickers, starts, ends)}
            logger.debug("Processed metadata for %d tickers.", len(self._ticker_ranges))
            
        except (KeyError, TypeError, ValueError, PolarsError) as e:
            logger.warning("Failed to process ticker metadata: %s. Smart batching disabled.", e)
            self._ticker_ranges = None

    # --------------------------------------------------------------------------
    # Properties 
    # --------------------------------------------------------------------------

    @property
    def provider(self) -> str:
        """Get the provider name.

        Returns:
            str: The provider identifier ('sharadar').
        """
        return "sharadar"

    @property
    def api_key(self) -> str:
        """Get the configured API key."""
        return self._api_key

    @property
    def rate_limit(self) -> int:
        """Get the configured rate limit."""
        return self._rate_limit

    # --------------------------------------------------------------------------
    # Abstract Methods (BaseRouter Implementation)
    # --------------------------------------------------------------------------

    async def generate_jobs(
        self, *, dataset: str, symbols: Sequence[str] | None, **kwargs: object
    ) -> AsyncIterator[FetchJob]:
        """Generate fetch jobs based on dataset and symbols.

        For 'tickers' dataset, generates a single job with pagination context.
        For other datasets, generates jobs per symbol or in bulks if supported.

        Args:
            dataset: Target dataset name (e.g., 'price', 'tickers').
            symbols: List of ticker symbols to fetch.
            **kwargs: Additional arguments:
                - per_page (int): Number of records per page (for tickers).
                - dimension (str): Dimension filter (e.g., 'MRT').
                - bulk_size (int): Bulk size for bulk datasets.

        Yields:
            FetchJob: A job object containing the request specification.
        """
        # -------- Build Pagination Jobs --------
        
        # Unified pagination handling:
        # - tickers: paginated only when symbols not provided
        # - sp500: paginated only when symbols not provided
        if (dataset == "tickers" and not symbols) or (
            dataset == "sp500" and not symbols
        ):
            # Sharadar API limit: maximum 10,000 rows per response
            per_page = self.MAX_ROWS_PER_REQUEST
            yield self._build_pagination_job(dataset=dataset, per_page=per_page)
            return

        if not symbols:
            return

        # -------- Build Per-Symbol Jobs --------
        
        symbol_list = [s.strip() for s in symbols if isinstance(s, str) and s.strip()]
        if not symbol_list:
            raise ValueError(f"SharadarRouter: no valid symbols provided from input={symbols!r}")

        raw_dimension = kwargs.get("dimension")
        if not raw_dimension or str(raw_dimension).strip() == "":
            dimension = "MRT"
        else:
            dimension = str(raw_dimension)

        # Refactored Strategy: Router-driven Batching with Dynamic Sizing
        # Two modes:
        # 1. Smart Batching (if metadata available): Pack based on exact row counts.
        # 2. Heuristic Batching (fallback): Fixed batch size based on date range.

        if self._ticker_ranges:
            # -------- Smart Batching Mode --------
            
            # Smart Batching Mode
            current_batch: list[str] = []
            current_rows = 0
            max_rows = self.MAX_ROWS_PER_REQUEST
            max_batch_size = self.DEFAULT_BATCH_SIZE  # API URL length safety
            
            for symbol in symbol_list:
                est_rows = self._estimate_ticker_rows(symbol, dataset)
                
                if est_rows > max_rows:
                    yield self._build_per_symbol_job(dataset=dataset, symbol=symbol, dimension=dimension)
                    continue
                
                if (current_rows + est_rows > max_rows) or (len(current_batch) >= max_batch_size):
                    if current_batch:
                        yield self._build_per_symbol_job(
                            dataset=dataset, 
                            symbol=",".join(current_batch), 
                            dimension=dimension
                        )
                    current_batch = []
                    current_rows = 0
                
                current_batch.append(symbol)
                current_rows += est_rows
                
            # Flush remaining
            if current_batch:
                yield self._build_per_symbol_job(
                    dataset=dataset, 
                    symbol=",".join(current_batch), 
                    dimension=dimension
                )
                
        else:
            # -------- Heuristic Batching Mode --------
            
            # Heuristic Batching Mode (Original Refactor)
            batch_size = self._calculate_batch_size(dataset)
            
            for chunk in self._iter_symbol_batches(symbol_list, batch_size):
                if not chunk:
                    continue
                batch_symbol_str = ",".join(chunk)
                
                yield self._build_per_symbol_job(
                    dataset=dataset, symbol=batch_symbol_str, dimension=dimension
                )

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        """Parse the API response payload into structured data.

        Decodes the JSON payload, converts it to a Polars DataFrame, validates columns,
        and handles pagination for 'tickers' dataset.

        Args:
            job: The fetch job associated with the response.
            payload: Raw bytes of the API response.

        Returns:
            ParseResult: Result containing data packets and any subsequent jobs.
        """
        # -------- Parse Response Payload --------
        
        # OPTIMIZATION: Try Polars native JSON parsing first (Release GIL, Zero-Copy)
        frame = pl.DataFrame()
        meta = {}

        try:
            # pl.read_json is significantly faster than json.loads for large data
            json_df = pl.read_json(io.BytesIO(payload))

            # -------- Handle API Errors --------
            
            if "quandl_error" in json_df.columns:
                err = json_df.select(pl.col("quandl_error")).item(0)
                if err:
                    code = err.get("code", "Unknown")
                    message = err.get("message", "Unknown error")
                    raise ValueError(f"Sharadar API error {code}: {message}")

            # -------- Extract & Transform Data --------
            
            # Extract Metadata
            if "meta" in json_df.columns and not json_df.is_empty():
                meta_col = json_df.select(pl.col("meta"))
                if not meta_col.is_empty():
                    try:
                        meta = meta_col.item(0, 0) or {}
                    except (ValueError, TypeError):
                        meta = {}

            # Extract column names from datatable structure
            if "datatable" not in json_df.columns:
                raise ValueError("Missing datatable in Sharadar response")
            dt_col = json_df.get_column("datatable")
            if dt_col.is_null().all():
                raise ValueError("Missing datatable in Sharadar response")
            cols_val = json_df.select(pl.col("datatable").struct.field("columns")).item(0, 0)
            if isinstance(cols_val, pl.Series):
                cols_list = cols_val.to_list()
            else:
                cols_list = cols_val
            cols_list = cols_list or []

            col_names = []
            for i, c in enumerate(cols_list):
                name = c.get("name")
                col_names.append(name if isinstance(name, str) and name else f"column_{i}")

            # Convert nested data to flat DataFrame
            frame = (
                json_df.select(pl.col("datatable").struct.field("data").alias("row"))
                .explode("row")
                .select(pl.col("row").list.to_struct(fields=col_names).struct.unnest())
            )

        except (pl.exceptions.PolarsError, ValueError, TypeError, json.JSONDecodeError) as e:
            # Fallback to standard JSON parsing if Polars fails
            logger.warning(
                "Polars JSON parse failed for %s, falling back to json.loads. Error: %s",
                job.dataset,
                e,
                exc_info=True,
            )
            decoded = self._decode_payload(payload)
            frame = self._datatable_to_frame(decoded, dataset=job.dataset)
            meta = decoded.get("meta") or {}

        # -------- Validate & Standardize Frame --------
        
        # Validate non-empty frame
        empty_result = self._check_empty_response(frame=frame)
        if empty_result:
            return empty_result

        # Map dataset to table name
        observed_at = datetime.now(tz=timezone.utc)
        table = DATASET_TABLE.get(job.dataset)
        if table is None:
            raise NotImplementedError(f"Unsupported dataset: {job.dataset}")
        
        # Add provider metadata
        frame = self._add_provider_metadata(frame=frame, observed_at=observed_at)
        
        packets = [
            FramePacket(
                provider=self.provider,
                table=table,
                frame=frame,
                observed_at=observed_at,
                context={"dataset": job.dataset},
            )
        ]

        # -------- Handle Pagination --------
        
        next_jobs = []
        if meta and isinstance(meta, dict):
            pagination = job.context.get("pagination")
            if pagination:
                meta_key = pagination.get("meta_key")
                cursor_param = pagination.get("cursor_param")
                next_cursor = meta.get(meta_key) if meta_key else None

                if (
                    next_cursor
                    and cursor_param
                    and next_cursor != job.spec.params.get(cursor_param)
                ):
                    new_job = job.model_copy(deep=True)
                    new_job.spec.params[cursor_param] = next_cursor
                    next_jobs.append(new_job)

        return ParseResult(packets=packets, next_jobs=next_jobs)

    # --------------------------------------
    # Generate Jobs Helpers
    # --------------------------------------
    
    # ------ Authentication: add API key as query parameter ------
    def _auth(self) -> RequestAuth:
        """Generate authentication details for the request.

        Returns:
            RequestAuth: Authentication object with API key in query parameters.
        """
        return RequestAuth(
            kind="query",
            token=self._api_key,
            query_param="api_key",
        )

    # ------ URL resolution: build dataset-specific endpoint ------
    def _dataset_url(self, dataset: str) -> str:
        """Resolve the full API URL for a given dataset.

        Args:
            dataset: The dataset name.

        Returns:
            str: Full API endpoint URL.

        Raises:
            NotImplementedError: If the dataset is not supported.
        """
        endpoint = DATASET_ENDPOINT.get(dataset)
        if endpoint is None:
            raise NotImplementedError(f"Unsupported dataset: {dataset}")
        return f"{self._BASE_URL}/{endpoint}.json"

    # ------ Column selection: derive request columns from schema ------
    def _get_request_columns(self, dataset: str) -> str:
        """Get the comma-separated list of columns to request from the API.

        Derives the column list from the schema definition, excluding internal columns.

        Args:
            dataset: The dataset name.

        Returns:
            str: Comma-separated column names.
        """
        schema = DATASET_SCHEMA.get(dataset)
        if not schema:
            return ""

        cols = [col for col in schema.schema.keys() if col not in INTERNAL_COLS]
        return ",".join(cols)

    # ------ Build pagination job: apply per_page/columns/date filters ------
    def _build_pagination_job(self, *, dataset: str, per_page: int = 10000) -> FetchJob:
        """Build a fetch job with pagination support for tickers or sp500.

        Args:
            per_page: Number of records to fetch per page.

        Returns:
            FetchJob: Job configured for fetching with pagination.
        """
        params = {"qopts.per_page": str(per_page)}
        if dataset == "tickers":
            params["qopts.columns"] = self._get_request_columns("tickers")
        elif dataset == "sp500":
            params["qopts.columns"] = self._get_request_columns("sp500")
        # Apply dataset-specific date filters
        date_col = DATE_FILTER_COL.get(dataset)
        if date_col:
            if self._start_date:
                params[f"{date_col}.gte"] = self._start_date
            if self._end_date:
                params[f"{date_col}.lte"] = self._end_date

        req = RequestSpec(
            method=HttpMethod.GET,
            url=self._dataset_url(dataset),
            params=params,
            auth=self._auth(),
        )
        context = copy.deepcopy(self._PAGINATION_CONTEXT)
        return FetchJob(
            provider=self.provider,
            dataset=dataset,
            symbol=None,
            spec=req,
            context=context,
        )

    # ------ Build per-symbol job: validate ticker and set dataset params ------
    def _build_per_symbol_job(
        self, *, dataset: str, symbol: str, dimension: str = "MRT"
    ) -> FetchJob:
        """Build a fetch job for a specific symbol (or batch of symbols).

        Args:
            dataset: Target dataset name.
            symbol: Ticker symbol or comma-separated list of symbols.
            dimension: Dimension parameter (used for fundamental data).

        Returns:
            FetchJob: Job configured for the specific symbol(s).
        """
        params: dict[str, str] = {}

        # Validate symbol to prevent empty ticker requests
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(f"Invalid symbol for per-symbol job: '{symbol}'")

        clean_symbol = symbol.strip()
        params["ticker"] = clean_symbol

        if dataset == "price":
            params["qopts.columns"] = self._get_request_columns("price")
        elif dataset == "fundamental":
            params["dimension"] = dimension
        elif dataset == "insider":
            params["qopts.columns"] = self._get_request_columns("insider")

        # Dataset-specific date filters (Datatables require actual column names)
        date_col = DATE_FILTER_COL.get(dataset)
        if date_col:
            if self._start_date:
                params[f"{date_col}.gte"] = self._start_date
            if self._end_date:
                params[f"{date_col}.lte"] = self._end_date

        req = RequestSpec(
            method=HttpMethod.GET,
            url=self._dataset_url(dataset),
            params=params,
            auth=self._auth(),
        )

        # Add pagination context for all datasets that support it
        context = copy.deepcopy(self._PAGINATION_CONTEXT)

        return FetchJob(
            provider=self.provider,
            dataset=dataset,
            symbol=symbol,
            spec=req,
            context=context,
        )
        
    # ------ Rows per ticker: convert days→rows by dataset characteristics ------
    def _calculate_rows_per_ticker(self, days: int, dataset: str) -> int:
        """Calculate estimated rows per ticker based on dataset type and days."""
        if dataset in ("price", "daily"):
            return max(1, int(days * self.TRADING_DAYS_RATIO))
        elif dataset in ("fundamental", "insider", "institutional"):
            return max(1, int(days * self.QUARTERLY_DAYS_RATIO))
        else:
            return days

    # ------ Batch size: optimize to respect 10,000-row API limit ------
    def _calculate_batch_size(self, dataset: str) -> int:
        """Calculate optimal batch size based on date range and dataset.
        
        Estimates the number of rows per ticker to keep the total response size
        under the 10,000 row limit per request (Sharadar API limit).
        
        Args:
            dataset: The dataset name (e.g., 'price', 'fundamental').
            
        Returns:
            int: The calculated batch size (number of tickers per request).
        """
        # If no date range provided, estimate full history days and compute batch size.
        # Conservative cap applied to avoid overly small batches for long histories.
        if not self._start_date:
            est_days = min(30 * 252, 10_000)  # ~30 years of trading days capped
            rows_per_ticker = self._calculate_rows_per_ticker(est_days, dataset)
            batch_size = self.MAX_ROWS_PER_REQUEST // max(1, rows_per_ticker)
            # Clamp between MIN and DEFAULT to avoid extremes.
            clamped = max(self.MIN_BATCH_SIZE, min(self.DEFAULT_BATCH_SIZE, batch_size))
            # Extremely large histories may still warrant MIN_BATCH_SIZE; keep conservative bound.
            return clamped
            
        # Use BaseRouter helper to parse date range
        try:
            date_range = self._parse_date_range(self._start_date, self._end_date)
        except ValueError:
            date_range = None
        if date_range is None:
            # Default to conservative minimal batch when range parsing fails.
            return self.MIN_BATCH_SIZE
            
        start, end = date_range
        days = (end - start).days
        days = max(1, days)  # Ensure at least 1 day
            
        # Use common calculation method
        rows_per_ticker = self._calculate_rows_per_ticker(days, dataset)
        
        # Calculate batch size
        # Example: 1 year daily data = ~250 rows. 10000 / 250 = 40 tickers.
        batch_size = self.MAX_ROWS_PER_REQUEST // rows_per_ticker
        
        # Apply bounds (1 <= batch_size <= 100)
        # Sharadar recommends max 100 tickers per request for URL length safety
        return max(self.MIN_BATCH_SIZE, min(self.DEFAULT_BATCH_SIZE, batch_size))

    # ------ Rows estimate (metadata): compute overlap of request vs. ticker range ------
    def _estimate_ticker_rows(self, ticker: str, dataset: str) -> int:
        """Estimate exact row count for a specific ticker using metadata."""
        # Fallback to heuristic if no metadata or ticker not found
        if not self._ticker_ranges or ticker not in self._ticker_ranges:
            if not self._start_date:
                return self.MAX_ROWS_PER_REQUEST  # Assume full history is huge -> forces single batch
                
            # Use BaseRouter helper to parse date range
            try:
                date_range = self._parse_date_range(self._start_date, self._end_date)
            except ValueError:
                date_range = None
            if date_range is None:
                return self.MAX_ROWS_PER_REQUEST
                
            start, end = date_range
            days = (end - start).days
            days = max(1, days)  # Ensure at least 1 day
            
            return self._calculate_rows_per_ticker(days, dataset)

        # Smart Calculation using metadata
        ticker_start, ticker_end = self._ticker_ranges[ticker]
        
        # Ensure ticker dates are date objects (handle datetime)
        if isinstance(ticker_start, datetime):
            ticker_start = ticker_start.date()
        if isinstance(ticker_end, datetime):
            ticker_end = ticker_end.date()

        # Parse request range
        try:
            req_range = self._parse_date_range(self._start_date, self._end_date)
        except ValueError:
            req_range = None
        if req_range is None:
            req_start, req_end = date.min, date.today()
        else:
            req_start, req_end = req_range
            if isinstance(req_start, datetime):
                req_start = req_start.date()
            if isinstance(req_end, datetime):
                req_end = req_end.date()
        
        # Calculate overlap
        overlap_start = max(req_start, ticker_start)
        overlap_end = min(req_end, ticker_end)
        
        if overlap_start > overlap_end:
            return 0  # No data in range
            
        days = (overlap_end - overlap_start).days
        days = max(1, days)  # Ensure at least 1 day for inclusive calculation
        
        return self._calculate_rows_per_ticker(days, dataset)

    # ------ Batch splitting: split symbol sequence into fixed-size chunks ------
    def _iter_symbol_batches(
        self, symbols: Sequence[str], batch_size: int
    ) -> Iterator[list[str]]:
        """Yield batches of symbols from the sequence.

        Args:
            symbols: Sequence of symbols.
            batch_size: Size of each batch.

        Yields:
            list[str]: A list of symbols for the current batch.
        """
        for idx in range(0, len(symbols), batch_size):
            yield list(symbols[idx : idx + batch_size])

    # --------------------------------------
    # Parse Helpers 
    # --------------------------------------

    # ------ Payload decode: JSON→dict and API error checks ------
    def _decode_payload(self, payload: bytes) -> dict:
        """Decode the response bytes into a dictionary.

        Args:
            payload: Response bytes.

        Returns:
            dict: Decoded JSON dictionary.

        Raises:
            ValueError: If the API returns a 'quandl_error'.
        """
        decoded = json.loads(payload.decode("utf-8"))
        if "quandl_error" in decoded:
            err = decoded.get("quandl_error") or {}
            code = err.get("code", "Unknown")
            message = err.get("message", "Unknown error")
            raise ValueError(f"Sharadar API error {code}: {message}")
        return decoded

    # ------ Frame load: datatable(columns/data)→Polars DataFrame ------
    def _datatable_to_frame(self, decoded: dict, dataset: str) -> pl.DataFrame:
        """Convert the 'datatable' portion of the response to a DataFrame.

        Args:
            decoded: Decoded JSON response.
            dataset: The dataset name to apply schema for.

        Returns:
            pl.DataFrame: Polars DataFrame containing the data.
        """
        datatable = decoded.get("datatable") or {}
        records = datatable.get("data") or []
        columns = datatable.get("columns") or []
        if not records or not columns:
            return pl.DataFrame()

        col_names: list[str] = []
        for i, c in enumerate(columns):
            name = c.get("name")
            col_names.append(name if isinstance(name, str) and name else f"column_{i}")

        # 1. Load DataFrame
        # If target schema is known, load everything as Utf8 first to prevent inference errors
        # (e.g., empty strings in numeric columns). If unknown, rely on Polars inference.
        target_schema = DATASET_SCHEMA.get(dataset)
        schema_arg = (
            {name: pl.Utf8 for name in col_names} if target_schema else col_names
        )

        frame = pl.DataFrame(records, schema=schema_arg, orient="row")

        return frame
    
    
    
