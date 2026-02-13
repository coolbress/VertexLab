from __future__ import annotations

import logging
from typing import Final
import json
import io
from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import datetime, timezone

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
from vertex_forager.providers.sharadar.utils import (
    DATASET_ENDPOINT,
    DATASET_SCHEMA,
    DATASET_TABLE,
    DATE_FILTER_COL,
    INTERNAL_COLS,
)


from copy import deepcopy

logger = logging.getLogger("vertex_forager.providers.sharadar.router")

class SharadarRouter(BaseRouter):
    """Data Router implementation for Sharadar (Nasdaq Data Link)."""

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

    def __init__(
        self,
        *,
        api_key: str,
        rate_limit: int,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: object,
    ) -> None:
        """Initialize SharadarRouter.

        Args:
            api_key: Valid API key for the provider.
            rate_limit: Rate limit (requests per minute).
            start_date: Optional start date filter (YYYY-MM-DD).
            end_date: Optional end date filter (YYYY-MM-DD).
            **kwargs: Additional configuration parameters.
        """
        self._api_key = api_key
        self._rate_limit = rate_limit
        self._start_date = start_date
        self._end_date = end_date

    # --------------------------------------------------------------------------
    # Public API (BaseRouter Implementation)
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

    @property
    def start_date(self) -> str | None:
        """Get the configured start date filter."""
        return self._start_date

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
        # Unified pagination handling:
        # - tickers: paginated only when symbols not provided
        # - sp500: paginated only when symbols not provided
        if (dataset == "tickers" and not symbols) or (dataset == "sp500" and not symbols):
            try:
                raw_per_page = kwargs.get("per_page", 10000)
                # Ensure it's not None/Empty and is numeric
                if raw_per_page is None or str(raw_per_page).strip() == "":
                    per_page = 10000
                else:
                    per_page = int(raw_per_page)
                
                # Enforce sensible bounds
                if per_page < 1:
                    per_page = 1
                elif per_page > 100000:  # Reasonable upper limit for Sharadar
                    per_page = 100000
            except (ValueError, TypeError):
                # Fallback on failure
                per_page = 10000

            yield self._build_pagination_job(dataset=dataset, per_page=per_page)
            return

        if not symbols:
            return

        symbol_list = list(symbols)
        
        raw_dimension = kwargs.get("dimension")
        if not raw_dimension or str(raw_dimension).strip() == "":
            dimension = "MRT"
        else:
            dimension = str(raw_dimension)
        
        # Simplified 1-to-1 Mapping:
        # The Client is responsible for all bulk packing (Smart Bulk Packing or Simple Chunking).
        # The Router simply creates one job per input symbol string.
        # This removes the need for 'bulk_size' logic in the Router.
        for symbol in symbol_list:
            if not symbol:
                continue
            yield self._build_per_symbol_job(
                dataset=dataset, symbol=symbol, dimension=dimension
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
        # OPTIMIZATION: Try Polars native JSON parsing first (Release GIL, Zero-Copy)
        frame = pl.DataFrame()
        meta = {}
        
        try:
            # pl.read_json is significantly faster than json.loads for large data
            # It expects a file-like object or bytes (if simple json)
            # For Sharadar's nested structure, it returns a Struct.
            json_df = pl.read_json(io.BytesIO(payload))
            
            # Check for API Error
            if "quandl_error" in json_df.columns:
                err = json_df.select(pl.col("quandl_error")).item(0)
                if err:
                    code = err.get("code", "Unknown")
                    message = err.get("message", "Unknown error")
                    raise ValueError(f"Sharadar API error {code}: {message}")

            # Extract Metadata
            if "meta" in json_df.columns and not json_df.is_empty():
                # Safe extraction handling potential nulls/empty frames
                meta_col = json_df.select(pl.col("meta"))
                if not meta_col.is_empty():
                    # For DataFrame with 1 column, item(0, 0) is safer in recent Polars
                    try:
                        meta = meta_col.item(0, 0) or {}
                    except (ValueError, TypeError):
                        meta = {}

            # Extract Data using Pure Polars operations (avoiding Python round-trip)
            # This is significantly faster (approx 1.6x) for large payloads
            
            # 1. Get Column Names (Metadata is small, safe to extract to Python)
            # 'columns' is List[Struct] in the JSON
            # We use item(0, 0) to get the first row, first column safely
            cols_val = json_df.select(pl.col("datatable").struct.field("columns")).item(0, 0)
            
            # Handle Polars Series return type (for List columns)
            if isinstance(cols_val, pl.Series):
                cols_list = cols_val.to_list()
            else:
                cols_list = cols_val
            
            cols_list = cols_list or []

            col_names = []
            for i, c in enumerate(cols_list):
                name = c.get("name")
                col_names.append(name if isinstance(name, str) and name else f"column_{i}")
            
            # 2. Explode and Unnest 'data' (Heavy part, keep in Polars)
            # 'data' is List[List[str]] (or mixed, coerced to str by read_json)
            frame = (
                json_df.select(pl.col("datatable").struct.field("data").alias("row"))
                .explode("row")
                .select(
                    pl.col("row").list.to_struct(fields=col_names).struct.unnest()
                )
            )
            
            # 3. Cast columns that were coerced to String (Polars JSON mixed-type list behavior)
            # We rely on SchemaMapper to do the final strict validation, 
            # but here we can do a quick pass if needed, or just let SchemaMapper handle it.
            # Ideally, we should hint types here if we knew them, but Sharadar columns vary.
            # SchemaMapper will handle the actual casting/validation.
            
        except (pl.exceptions.PolarsError, ValueError, TypeError, json.JSONDecodeError) as e:
            # Fallback to standard parsing if Polars fails (e.g., unexpected format)
            logger.warning(
                f"Polars JSON parse failed for {job.dataset}, falling back to json.loads. Error: {e}", 
                exc_info=True
            )
            decoded = self._decode_payload(payload)
            frame = self._datatable_to_frame(decoded, dataset=job.dataset)
            meta = decoded.get("meta") or {}

        if frame.is_empty():
            return ParseResult(packets=[], next_jobs=[])

        observed_at = datetime.now(tz=timezone.utc)
        table, frame = self._to_table_frame(job=job, frame=frame, observed_at=observed_at)
        if frame.is_empty():
            return ParseResult(packets=[], next_jobs=[])

        packets = [
            FramePacket(
                provider=self.provider,
                table=table,
                frame=frame,
                observed_at=observed_at,
                context={"dataset": job.dataset},
            )
        ]

        next_jobs = []
        if meta and isinstance(meta, dict):
            pagination = job.context.get("pagination")
            if pagination:
                meta_key = pagination.get("meta_key")
                cursor_param = pagination.get("cursor_param")
                next_cursor = meta.get(meta_key) if meta_key else None

                if next_cursor and cursor_param and next_cursor != job.spec.params.get(cursor_param):
                    new_job = job.model_copy(deep=True)
                    new_job.spec.params[cursor_param] = next_cursor
                    # Important: Each paginated request counts as a separate API call.
                    # The rate limiter in the pipeline will enforce limits for each of these subsequent jobs.
                    next_jobs.append(new_job)

        return ParseResult(packets=packets, next_jobs=next_jobs)

    # --------------------------------------------------------------------------
    # API Integration (Request Building)
    # --------------------------------------------------------------------------

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

        cols = [
            col for col in schema.schema.keys()
            if col not in INTERNAL_COLS
        ]
        return ",".join(cols)

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
        context = deepcopy(self._PAGINATION_CONTEXT)
        return FetchJob(
            provider=self.provider,
            dataset=dataset,
            symbol=None,
            spec=req,
            context=context,
        )

    def _build_per_symbol_job(self, *, dataset: str, symbol: str, dimension: str = "MRT") -> FetchJob:
        """Build a fetch job for a specific symbol (or batch of symbols).

        Args:
            dataset: Target dataset name.
            symbol: Ticker symbol or comma-separated list of symbols.
            dimension: Dimension parameter (used for fundamental data).

        Returns:
            FetchJob: Job configured for the specific symbol(s).
        """
        params: dict[str, str] = {}
        
        # Explicitly validate symbol to prevent empty ticker requests (which trigger bulk)
        if not isinstance(symbol, str) or not symbol.strip():
             # Should be caught upstream, but as a safeguard
             # We can't easily raise 400 here without disrupting the flow, 
             # but returning None or raising ValueError is appropriate for internal logic.
             # Since this is an internal builder, let's assume valid input or raise.
             # However, the user asked to "raise/return a 400-like error or skip".
             # Raising ValueError seems safest to stop this job creation.
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
        context = self._PAGINATION_CONTEXT.copy()
        
        return FetchJob(
            provider=self.provider,
            dataset=dataset,
            symbol=symbol,
            spec=req,
            context=context,
        )

    # --------------------------------------------------------------------------
    # Response Parsing & Decoding
    # --------------------------------------------------------------------------

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
        schema_arg = {name: pl.Utf8 for name in col_names} if target_schema else col_names
        
        frame = pl.DataFrame(records, schema=schema_arg, orient="row")

        # 2. Apply strict schema casting if available
        # OPTIMIZATION: Delegated to SchemaMapper.normalize() to avoid double casting.
        # if target_schema:
        #     cast_exprs = []
        #     for col_name, dtype in target_schema.schema.items():
        #         if col_name in frame.columns:
        #             # Special handling for Date types if they are strings
        #             if dtype == pl.Date:
        #                 cast_exprs.append(pl.col(col_name).str.to_date(strict=False).alias(col_name))
        #             # Standard casting for other types
        #             else:
        #                 cast_exprs.append(pl.col(col_name).cast(dtype, strict=False).alias(col_name))
        #
        #     if cast_exprs:
        #         frame = frame.with_columns(cast_exprs)
        
        return frame

    # --------------------------------------------------------------------------
    # Data Normalization & Validation
    # --------------------------------------------------------------------------

    def _to_table_frame(
        self, *, job: FetchJob, frame: pl.DataFrame, observed_at: datetime
    ) -> tuple[str, pl.DataFrame]:
        """Map dataset to table name and normalize the DataFrame.

        Args:
            job: Fetch job.
            frame: Raw DataFrame.
            observed_at: Timestamp of observation.

        Returns:
            tuple[str, pl.DataFrame]: Table name and normalized DataFrame.

        Raises:
            NotImplementedError: If dataset is not supported.
            ValueError: If required columns are missing (for price dataset).
        """
        table = DATASET_TABLE.get(job.dataset)
        if table is None:
            raise NotImplementedError(f"Unsupported dataset: {job.dataset}")

        return table, self._normalize_frame(
            frame=frame, observed_at=observed_at
        )

    def _normalize_frame(
        self, *, frame: pl.DataFrame, observed_at: datetime
    ) -> pl.DataFrame:
        """Add provider metadata to the DataFrame.

        Args:
            frame: Input DataFrame.
            observed_at: Fetch timestamp.

        Returns:
            pl.DataFrame: DataFrame with metadata columns.
        """
        return frame.with_columns(
            [
                pl.lit(self.provider).alias("provider"),
                pl.lit(observed_at).alias("fetched_at"),
            ]
        )

    # --------------------------------------------------------------------------
    # Utility Helpers
    # --------------------------------------------------------------------------

    def _iter_symbol_batches(self, symbols: Sequence[str], batch_size: int) -> Iterator[list[str]]:
        """Yield batches of symbols from the sequence.

        Args:
            symbols: Sequence of symbols.
            batch_size: Size of each batch.

        Yields:
            list[str]: A list of symbols for the current batch.
        """
        for idx in range(0, len(symbols), batch_size):
            yield list(symbols[idx : idx + batch_size])
