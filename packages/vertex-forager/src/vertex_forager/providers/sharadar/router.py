from __future__ import annotations

import logging
from typing import Final
import uuid
import json
import io
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Mapping
from datetime import date, datetime, timezone

import polars as pl

from typing import Any, cast, TYPE_CHECKING
from vertex_forager.core.types import SharadarDataset, JSONValue
from vertex_forager.providers.sharadar.constants import (
    MAX_ROWS_PER_REQUEST,
    DEFAULT_BATCH_SIZE,
    MIN_BATCH_SIZE,
    TRADING_DAYS_RATIO,
    QUARTERLY_DAYS_RATIO,
    PAGINATION_META_KEY,
    PAGINATION_CURSOR_PARAM,
    MAX_PAGES,
)
from vertex_forager.constants import TRADING_DAYS_PER_YEAR
if TYPE_CHECKING:
    from vertex_forager.core.types import PerSymbolJobContext

from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    RequestAuth,
)
from vertex_forager.core.contracts import ParseResult
from vertex_forager.routers.base import BaseRouter
from vertex_forager.routers.errors import raise_quandl_error
from vertex_forager.routers.jobs import (
    pagination_job,
    single_symbol_job,
    make_pagination_context,
    build_symbol_context,
)
from polars.exceptions import PolarsError
from vertex_forager.providers.sharadar.schema import (
    DATASET_SCHEMA,
    DATASET_TABLE,
)
from vertex_forager.providers.sharadar.constants import (
    DATASET_ENDPOINT,
    DATE_FILTER_COL,
    INTERNAL_COLS,
    API_KEY_QUERY_PARAM,
    QOPTS_PER_PAGE,
    QOPTS_COLUMNS,
)
from vertex_forager.logging.constants import (
    ROUTER_LOG_PREFIX,
    LOG_META_MISSING_COLS,
    LOG_META_PROCESSED,
    LOG_META_PROCESS_FAIL,
    LOG_UNSUPPORTED_DATASET,
    LOG_PAGINATION_START,
    LOG_BATCH_FORCE_SINGLE,
    LOG_BATCH_FLUSH,
    LOG_BATCH_ADD,
    LOG_HEURISTIC_BATCH_SIZE,
    LOG_BUILD_JOB,
)


logger = logging.getLogger("vertex_forager.providers.sharadar.router")


class SharadarRouter(BaseRouter[SharadarDataset]):
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
            "cursor_param": PAGINATION_CURSOR_PARAM,
            "meta_key": PAGINATION_META_KEY,
            "max_pages": MAX_PAGES,
        }
    }
    PAGINATION_META_KEY: Final[str] = PAGINATION_META_KEY
    PAGINATION_CURSOR_PARAM: Final[str] = PAGINATION_CURSOR_PARAM
    MAX_PAGES: Final[int] = MAX_PAGES

    # Constants for batch calculation and API limits
    MAX_ROWS_PER_REQUEST: Final[int] = MAX_ROWS_PER_REQUEST
    DEFAULT_BATCH_SIZE: Final[int] = DEFAULT_BATCH_SIZE
    MIN_BATCH_SIZE: Final[int] = MIN_BATCH_SIZE
    TRADING_DAYS_RATIO: Final[float] = TRADING_DAYS_RATIO
    QUARTERLY_DAYS_RATIO: Final[float] = QUARTERLY_DAYS_RATIO

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
                logger.warning(LOG_META_MISSING_COLS.format(prefix=ROUTER_LOG_PREFIX, required=required))
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
            logger.debug(LOG_META_PROCESSED.format(prefix=ROUTER_LOG_PREFIX, count=len(self._ticker_ranges)))
            
        except (KeyError, TypeError, ValueError, PolarsError) as e:
            logger.warning(LOG_META_PROCESS_FAIL.format(prefix=ROUTER_LOG_PREFIX, error=e))
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
        self, *, dataset: SharadarDataset, symbols: Sequence[str] | None, **kwargs: object
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
        trace_id = uuid.uuid4().hex
        req_id = 0
        if (dataset == "tickers" and not symbols) or (
            dataset == "sp500" and not symbols
        ):
            # Sharadar API limit: maximum 10,000 rows per response
            per_page_obj = kwargs.get("per_page", MAX_ROWS_PER_REQUEST)
            try:
                per_page = int(per_page_obj) if isinstance(per_page_obj, (int, str)) else MAX_ROWS_PER_REQUEST
            except (TypeError, ValueError):
                per_page = MAX_ROWS_PER_REQUEST
            per_page = max(1, min(MAX_ROWS_PER_REQUEST, per_page))
            logger.debug(LOG_PAGINATION_START.format(prefix=ROUTER_LOG_PREFIX, dataset=dataset, per_page=per_page))
            page_ctx = make_pagination_context(
                dataset=dataset,
                meta_key=PAGINATION_META_KEY,
                cursor_param=PAGINATION_CURSOR_PARAM,
                max_pages=MAX_PAGES,
            )
            page_ctx["trace_id"] = trace_id
            page_ctx["request_id"] = req_id
            req_id += 1
            yield pagination_job(
                provider=self.provider,
                dataset=dataset,
                url=self._dataset_url(dataset),
                params={QOPTS_PER_PAGE: str(per_page)},
                auth=self._auth(),
                context=page_ctx,
            )
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
            max_rows = MAX_ROWS_PER_REQUEST
            max_batch_size = DEFAULT_BATCH_SIZE  # API URL length safety
            
            for symbol in symbol_list:
                est_rows = self._estimate_ticker_rows(symbol, dataset)
                
                if est_rows > max_rows:
                    logger.debug(LOG_BATCH_FORCE_SINGLE.format(prefix=ROUTER_LOG_PREFIX, symbol=symbol, est_rows=est_rows, max_rows=max_rows))
                    yield self._build_per_symbol_job(dataset=dataset, symbol=symbol, dimension=dimension, extra_context={"trace_id": trace_id, "request_id": req_id})
                    req_id += 1
                    continue
                
                if (current_rows + est_rows > max_rows) or (len(current_batch) >= max_batch_size):
                    if current_batch:
                        logger.debug(LOG_BATCH_FLUSH.format(prefix=ROUTER_LOG_PREFIX, size=len(current_batch), rows=current_rows))
                        sym = ",".join(current_batch)
                        yield self._build_per_symbol_job(dataset=dataset, symbol=sym, dimension=dimension, extra_context={"trace_id": trace_id, "request_id": req_id})
                        req_id += 1
                    current_batch = []
                    current_rows = 0
                
                current_batch.append(symbol)
                current_rows += est_rows
                logger.debug(LOG_BATCH_ADD.format(prefix=ROUTER_LOG_PREFIX, symbol=symbol, est_rows=est_rows, current_rows=current_rows))
                
            # Flush remaining
            if current_batch:
                logger.debug(LOG_BATCH_FLUSH.format(prefix=ROUTER_LOG_PREFIX, size=len(current_batch), rows=current_rows))
                sym = ",".join(current_batch)
                yield self._build_per_symbol_job(dataset=dataset, symbol=sym, dimension=dimension, extra_context={"trace_id": trace_id, "request_id": req_id})
                req_id += 1
                
        else:
            # -------- Heuristic Batching Mode --------
            
            # Heuristic Batching Mode (Original Refactor)
            batch_size = self._calculate_batch_size(dataset)
            logger.debug(LOG_HEURISTIC_BATCH_SIZE.format(prefix=ROUTER_LOG_PREFIX, dataset=dataset, batch_size=batch_size))
            
            for chunk in self._iter_symbol_batches(symbol_list, batch_size):
                if not chunk:
                    continue
                batch_symbol_str = ",".join(chunk)
                
                logger.debug(LOG_BUILD_JOB.format(prefix=ROUTER_LOG_PREFIX, dataset=dataset, symbols=batch_symbol_str))
                yield self._build_per_symbol_job(dataset=dataset, symbol=batch_symbol_str, dimension=dimension, extra_context={"trace_id": trace_id, "request_id": req_id})
                req_id += 1

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
        meta: dict[str, object] = {}

        try:
            # pl.read_json is significantly faster than json.loads for large data
            json_df = pl.read_json(io.BytesIO(payload))

            # -------- Handle API Errors --------
            
            if "quandl_error" in json_df.columns:
                err = json_df.select(pl.col("quandl_error")).item(0)
                if err:
                    raise_quandl_error(self.provider, err)

            # -------- Extract & Transform Data --------
            
            # Extract Metadata
            if "meta" in json_df.columns and not json_df.is_empty():
                meta_col = json_df.select(pl.col("meta"))
                if not meta_col.is_empty():
                    try:
                        val = meta_col.item(0, 0)
                        meta = val if isinstance(val, dict) else {}
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
        
        pkt_ctx: dict[str, object] = {"dataset": job.dataset}
        if isinstance(job.context, dict):
            for k in ("symbol", "trace_id", "request_id"):
                if k in job.context:
                    pkt_ctx[k] = job.context[k]
        packets: list[FramePacket] = [
            FramePacket(
                provider=self.provider,
                table=table,
                frame=frame,
                observed_at=observed_at,
                context=cast("Mapping[str, JSONValue]", pkt_ctx),
            )
        ]

        # -------- Handle Pagination --------
        
        next_jobs = []
        if meta and isinstance(meta, dict):
            pagination = cast(dict[str, object] | None, job.context.get("pagination"))
            if pagination:
                meta_key_obj = pagination.get("meta_key")
                cursor_param_obj = pagination.get("cursor_param")
                meta_key = None if meta_key_obj is None else str(meta_key_obj)
                cursor_param = None if cursor_param_obj is None else str(cursor_param_obj)
                next_cursor = meta.get(meta_key) if meta_key is not None else None

                if (
                    next_cursor is not None
                    and cursor_param
                    and next_cursor != job.spec.params.get(cursor_param)
                ):
                    new_job = job.model_copy(deep=True)
                    if isinstance(cursor_param, str):
                        new_job.spec.params[cursor_param] = cast(JSONValue, next_cursor)
                        # Update per-request correlation id for pagination follow-ups (use int consistently)
                        try:
                            ctx_dict = dict(cast(dict[str, JSONValue], new_job.context))
                            old_req = ctx_dict.get("request_id")
                            new_req: int
                            if isinstance(old_req, int):
                                new_req = old_req + 1
                            else:
                                new_req = 1
                            ctx_dict["request_id"] = new_req
                            new_job.context = ctx_dict
                        except (TypeError, ValueError, AttributeError) as exc:
                            logger.warning("%s: Failed to update request_id in pagination follow-up: %s", ROUTER_LOG_PREFIX, exc)
                        except Exception:
                            logger.exception("%s: Unexpected error updating request_id in pagination follow-up", ROUTER_LOG_PREFIX)
                            raise
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
        token = self._api_key
        if token:
            logger.debug(f"{ROUTER_LOG_PREFIX}: Auth token configured")
        return RequestAuth(kind="query", token=token, query_param=API_KEY_QUERY_PARAM)

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
            raise NotImplementedError(LOG_UNSUPPORTED_DATASET.format(prefix=ROUTER_LOG_PREFIX, dataset=dataset))
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
    def _build_pagination_job(self, *, dataset: SharadarDataset, per_page: int = MAX_ROWS_PER_REQUEST) -> FetchJob:
        """Build a fetch job with pagination support for tickers or sp500.

        Args:
            per_page: Number of records to fetch per page.

        Returns:
            FetchJob: Job configured for fetching with pagination.
        """
        params = {QOPTS_PER_PAGE: str(per_page)}
        if dataset == "tickers":
            params[QOPTS_COLUMNS] = self._get_request_columns("tickers")
        elif dataset == "sp500":
            params[QOPTS_COLUMNS] = self._get_request_columns("sp500")
        # Apply dataset-specific date filters
        date_col = DATE_FILTER_COL.get(dataset)
        if date_col:
            if self._start_date:
                params[f"{date_col}.gte"] = self._start_date
            if self._end_date:
                params[f"{date_col}.lte"] = self._end_date
        

        context = make_pagination_context(
            dataset=dataset,
            meta_key=PAGINATION_META_KEY,
            cursor_param=PAGINATION_CURSOR_PARAM,
            max_pages=MAX_PAGES,
        )
        return pagination_job(
            provider=self.provider,
            dataset=dataset,
            url=self._dataset_url(dataset),
            params=params,
            auth=self._auth(),
            context=context,
        )

    # ------ Build per-symbol job: validate ticker and set dataset params ------
    def _build_per_symbol_job(
        self, *, dataset: SharadarDataset, symbol: str, dimension: str = "MRT", extra_context: dict[str, JSONValue] | None = None
    ) -> FetchJob:
        """Build a fetch job for a specific symbol (or batch of symbols).

        Args:
            dataset: Target dataset name.
            symbol: Ticker symbol or comma-separated list of symbols.
            dimension: Dimension parameter (used for fundamental data).
            extra_context: Optional dict[str, JSONValue] to propagate tracing/request
                metadata (e.g., trace_id, request_id) into the created FetchJob.
                Merged into the job context for downstream correlation.

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

        # Add pagination context for all datasets that support it
        page_ctx = make_pagination_context(
            dataset=dataset,
            meta_key=PAGINATION_META_KEY,
            cursor_param=PAGINATION_CURSOR_PARAM,
            max_pages=MAX_PAGES,
        )
        
        per_symbol_ctx = build_symbol_context(dataset=dataset, symbol=clean_symbol)
        final_ctx: dict[str, object] = {**per_symbol_ctx}
        if "pagination" in page_ctx:
            final_ctx["pagination"] = page_ctx["pagination"]
        if extra_context:
            final_ctx.update(extra_context)
            
        return single_symbol_job(
            provider=self.provider,
            dataset=dataset,
            symbol=clean_symbol,
            url=self._dataset_url(dataset),
            params=params,
            auth=self._auth(),
            context=cast("PerSymbolJobContext", final_ctx),
        )
        
    # ------ Rows per ticker: convert days→rows by dataset characteristics ------
    def _calculate_rows_per_ticker(self, days: int, dataset: str) -> int:
        """Calculate estimated rows per ticker based on dataset type and days."""
        if dataset in ("price", "daily"):
            return max(1, int(days * TRADING_DAYS_RATIO))
        elif dataset in ("fundamental", "insider", "institutional"):
            return max(1, int(days * QUARTERLY_DAYS_RATIO))
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
            est_days = min(30 * TRADING_DAYS_PER_YEAR, MAX_ROWS_PER_REQUEST)  # ~30 years trading days capped
            rows_per_ticker = self._calculate_rows_per_ticker(est_days, dataset)
            batch_size = MAX_ROWS_PER_REQUEST // max(1, rows_per_ticker)
            # Clamp between MIN and DEFAULT to avoid extremes.
            clamped = max(MIN_BATCH_SIZE, min(DEFAULT_BATCH_SIZE, batch_size))
            # Extremely large histories may still warrant MIN_BATCH_SIZE; keep conservative bound.
            return clamped
            
        # Use BaseRouter helper to parse date range
        try:
            date_range = self._parse_date_range(self._start_date, self._end_date)
        except ValueError:
            date_range = None
        if date_range is None:
            # Default to conservative minimal batch when range parsing fails.
            return MIN_BATCH_SIZE
            
        start, end = date_range
        days = (end - start).days
        days = max(1, days)  # Ensure at least 1 day
            
        # Use common calculation method
        rows_per_ticker = self._calculate_rows_per_ticker(days, dataset)
        
        # Calculate batch size
        # Example: 1 year daily data = ~250 rows. 10000 / 250 = 40 tickers.
        batch_size = MAX_ROWS_PER_REQUEST // rows_per_ticker
        
        # Apply bounds (1 <= batch_size <= 100)
        # Sharadar recommends max 100 tickers per request for URL length safety
        return max(MIN_BATCH_SIZE, min(DEFAULT_BATCH_SIZE, batch_size))

    # ------ Rows estimate (metadata): compute overlap of request vs. ticker range ------
    def _estimate_ticker_rows(self, ticker: str, dataset: str) -> int:
        """Estimate exact row count for a specific ticker using metadata."""
        # Fallback to heuristic if no metadata or ticker not found
        if not self._ticker_ranges or ticker not in self._ticker_ranges:
            if not self._start_date:
                return MAX_ROWS_PER_REQUEST  # Assume full history is huge -> forces single batch
                
            # Use BaseRouter helper to parse date range
            try:
                date_range = self._parse_date_range(self._start_date, self._end_date)
            except ValueError:
                date_range = None
            if date_range is None:
                return MAX_ROWS_PER_REQUEST
                
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
    def _decode_payload(self, payload: bytes) -> dict[str, Any]:
        """Decode the response bytes into a dictionary.

        Args:
            payload: Response bytes.

        Returns:
            dict: Decoded JSON dictionary.

        Raises:
            FetchError: If the API returns a 'quandl_error'.
        """
        decoded_any = json.loads(payload.decode("utf-8"))
        if not isinstance(decoded_any, dict):
            raise ValueError(f"Invalid response type for provider={self.provider}: expected object, got {type(decoded_any).__name__}")
        decoded = cast(dict[str, Any], decoded_any)
        if "quandl_error" in decoded:
            raw_err = decoded.get("quandl_error")
            if isinstance(raw_err, dict):
                err: dict[str, Any] = raw_err
            elif isinstance(raw_err, str):
                err = {"message": raw_err}
            else:
                err = {}
            raise_quandl_error(self.provider, err)
        return decoded

    # ------ Frame load: datatable(columns/data)→Polars DataFrame ------
    def _datatable_to_frame(self, decoded: dict[str, Any], dataset: str) -> pl.DataFrame:
        """Convert the 'datatable' portion of the response to a DataFrame.

        Args:
            decoded: Decoded JSON response.
            dataset: The dataset name to apply schema for.

        Returns:
            pl.DataFrame: Polars DataFrame containing the data.
        """
        datatable_any = decoded.get("datatable")
        if not isinstance(datatable_any, dict):
            raise ValueError(f"Invalid datatable for provider={self.provider} dataset={dataset}: expected object")
        datatable = cast(dict[str, Any], datatable_any)
        records_any = datatable.get("data")
        columns_any = datatable.get("columns")
        # Validate records/columns presence and types
        if records_any is None:
            return pl.DataFrame()
        if not isinstance(records_any, list):
            raise ValueError(f"Invalid datatable.data for provider={self.provider} dataset={dataset}: expected list")
        records: list[Any] = records_any
        if len(records) == 0:
            return pl.DataFrame()
        if columns_any is None or not isinstance(columns_any, list):
            raise ValueError(f"Invalid or missing datatable.columns for provider={self.provider} dataset={dataset} (records present)")
        columns: list[Any] = columns_any

        col_names: list[str] = []
        for i, c in enumerate(columns):
            if not isinstance(c, dict):
                raise ValueError(f"Invalid column structure at index {i} for provider={self.provider} dataset={dataset}: expected object")
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
    
    
    
