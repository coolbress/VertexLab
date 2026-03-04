from __future__ import annotations

import logging
import os
import time
from abc import ABC
import asyncio
from contextlib import asynccontextmanager, AsyncExitStack, nullcontext
from functools import partial
from pathlib import Path
from typing import Any, Callable, AsyncGenerator, TypeVar, Generic, Union

import httpx
import warnings
import polars as pl
from tqdm.auto import tqdm

from vertex_forager.core.http import default_async_client
from vertex_forager.core.http import HttpExecutor as _HttpExecutor
from vertex_forager.core.pipeline import VertexForager as _VertexForager
from vertex_forager.core.config import EngineConfig, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.contracts import IRouter, IWriter, IMapper
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.writers.base import BaseWriter
from vertex_forager.writers import create_writer
from vertex_forager.utils import Spinner, create_pbar_updater, sanitize_field, env_int, env_bool
from vertex_forager.core.types import JSONValue, SharadarDataset, YFinanceDataset
HttpExecutor = _HttpExecutor
VertexForager = _VertexForager


logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Union[SharadarDataset, YFinanceDataset, str])

class BaseClient(ABC, Generic[T]):
    """
    Vendor-agnostic base client abstraction for the Vertex Forager pipeline.

    This class serves as the foundation for all provider-specific clients (e.g., SharadarClient).
    It encapsulates the core infrastructure required to execute data collection tasks independently
    of the underlying data source.

    Key Responsibilities:
    1. **Session Management**: Manages the lifecycle of the underlying HTTP client (`httpx.AsyncClient`)
       via async context managers, ensuring efficient connection pooling and resource cleanup.
    2. **Pipeline Orchestration**: Instantiates and executes the `VertexForager` pipeline, wiring together
       components like Routers, Writers, and Mappers.
    3. **Flow Control**: Initializes the `FlowController` to enforce global rate limits and concurrency
       policies across all pipeline operations.
    4. **Configuration**: Centralizes engine configuration (`EngineConfig`) handling.

    Design Principles:
    - **Provider-Agnostic**: Contains NO vendor-specific logic. All vendor details must be injected
      via Routers and Mappers.
    - **Composition over Inheritance**: While this is a base class, it primarily delegates work to
      composed components (FlowController, VertexForager) rather than relying on deep inheritance chains.

    Standardized Provider Implementation Pattern:
    All provider clients should follow this consistent structure for extensibility:
    
    1. **execute_collection()** - Unified data collection pipeline with:
       - Router creation with provider-specific configuration
       - Writer lifecycle management (DB storage or in-memory)
       - Progress tracking and result collection
       - Memory safety validation
    
    2. **Provider-specific characteristics** documented in docstrings:
       - API rate limits and batching strategies
       - Data source characteristics (coverage, update frequency)
       - Special handling requirements
       - Performance optimization techniques
    
    3. **Memory management** via common utilities:
       - validate_memory_usage() for safety checks
       - Provider-specific memory parameters
    
    4. **Error handling patterns**:
       - Rate limit handling via FlowController
       - Network retry logic via HttpExecutor
       - Graceful degradation for missing data

    Usage:
        Subclasses must implement specific methods (e.g., `get_price_data`) that define *what* to fetch,
        delegating the *how* to `self.run_pipeline()`. Follow the standardized patterns above for
        consistency across all providers.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize the base client infrastructure.

        Args:
            api_key: API key for the provider (optional, depends on provider).
            rate_limit: Maximum requests per minute (RPM) allowed for this client.
            **kwargs: Additional configuration parameters passed directly to `EngineConfig`.
        """
        self.api_key = api_key

        # Enforce rate_limit into EngineConfig
        config_params = kwargs.copy()
        config_params["requests_per_minute"] = rate_limit

        if ("metrics_enabled" not in config_params) or (config_params.get("metrics_enabled") is None):
            config_params["metrics_enabled"] = env_bool("VF_METRICS_ENABLED")
        if ("structured_logs" not in config_params) or (config_params.get("structured_logs") is None):
            config_params["structured_logs"] = env_bool("VF_STRUCTURED_LOGS")
        if ("log_verbose" not in config_params) or (config_params.get("log_verbose") is None):
            config_params["log_verbose"] = env_bool("VF_LOG_VERBOSE")
        if ("concurrency" not in config_params) or (config_params.get("concurrency") is None):
            ci = env_int("VF_CONCURRENCY")
            if ci is not None and ci > 0:
                config_params["concurrency"] = ci
        if ("flush_threshold_rows" not in config_params) or (config_params.get("flush_threshold_rows") is None):
            ft = env_int("VF_FLUSH_THRESHOLD_ROWS")
            if ft is not None and ft > 0:
                config_params["flush_threshold_rows"] = ft
        self._config = EngineConfig(**config_params)
        self._structured_logs = bool(self._config.structured_logs)
        self._log_verbose = bool(self._config.log_verbose)

        # Initialize FlowController for global rate limiting
        self.controller = FlowController(
            requests_per_minute=self._config.requests_per_minute,
            concurrency_limit=self._config.fetch_concurrency,
        )
        self.last_run: RunResult | None = None
        self._client: httpx.AsyncClient | None = None

    async def aclose(self) -> None:
        """Asynchronously close the underlying HTTP client to release resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "BaseClient":
        """Async context manager entry.

        Initializes the shared HTTP client.
        """
        if self._client is None:
            self._client = default_async_client()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Async context manager exit.

        Ensures the HTTP client is closed.
        """
        await self.aclose()

    @property
    def config(self) -> EngineConfig:
        """Public engine/client configuration.

        Returns:
            EngineConfig: The configuration object governing rate limits,
                concurrency, queue sizes, and thresholds used by this client.

        Notes:
            Read-only accessor. Callers should not mutate the returned object
            in place; prefer constructing a new EngineConfig or using factory
            helpers to apply changes.
        """
        return self._config

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get the active HTTP client.

        Raises:
            RuntimeError: If the client has not been initialized (not in context).
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Use 'async with client:'")
        return self._client

    async def run_async(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Execute a standard async HTTP request using the underlying client.
        
        Delegates directly to client.request().
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Use 'async with client:'")
        return await self._client.request(method, url, **kwargs)

    async def run_sync(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute a blocking (synchronous) function in a separate thread.
        
        This wrapper ensures that blocking library calls (like yfinance, pandas I/O)
        do not freeze the main asyncio event loop.
        
        Args:
            func: Callable to execute in a worker thread.
            *args: Positional arguments for the callable.
            **kwargs: Keyword arguments for the callable.
        
        Returns:
            Any: The return value of the callable.
        
        Raises:
            Exception: Any exception raised by the callable is propagated.
        """
        pfunc = partial(func, *args, **kwargs)
        return await asyncio.to_thread(pfunc)

    def _safe_int(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            logger.debug("bad attempt value: %s", value)
            return 0

    async def run_pipeline(
        self,
        *,
        router: IRouter,
        dataset: T,
        symbols: list[str] | None,
        writer: IWriter,
        mapper: IMapper,
        on_progress: Callable[..., None] | None = None,
        **kwargs: JSONValue,
    ) -> RunResult:
        """
        Run the VertexForager pipeline for the given router, dataset, and symbols.
        
        Args:
            router: Data router to fetch data from.
            dataset: Dataset name (e.g., "price").
            symbols: List of symbols to fetch data for. If None, fetch all symbols.
            writer: Data writer to persist the processed data.
            mapper: Schema mapper to transform connector data to sink schema.
            on_progress: Optional callback function called on each completed request.
            **kwargs: Additional arguments passed to the pipeline run method.
        
        Returns:
            RunResult: Summary of the pipeline run, including success/failure status.
        
        Raises:
            httpx.RequestError: If a network error occurs during fetching.
            httpx.HTTPStatusError: If an HTTP response returns non-2xx.
            ValidationError: If schema validation fails during writing.
            PrimaryKeyMissingError: When required PK columns are absent.
            PrimaryKeyNullError: When PK columns contain nulls.
        """
        async with self._http_client():
            http = HttpExecutor(client=self)
            pipeline = VertexForager(
                router=router,
                http=http,
                writer=writer,
                mapper=mapper,
                config=self._config,
                controller=self.controller,
            )

            from vertex_forager.clients.validation import filter_reserved_kwargs
            reserved = {"router", "dataset", "symbols", "writer", "mapper", "on_progress"}
            run_kwargs = filter_reserved_kwargs(kwargs, reserved)

            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=FutureWarning,
                    module=r"pandas(\.|$)",
                )
                warnings.filterwarnings(
                    "ignore",
                    category=FutureWarning,
                    module=r"yfinance(\.|$)",
                )
                if self._structured_logs:
                    sym_count = len(symbols or [])
                    attempt = self._safe_int(run_kwargs.get("attempt", 0))
                    msg_s = f"OBS provider={sanitize_field(router.provider)} dataset={sanitize_field(dataset)} symbol=* symbols={sym_count} stage=client_run_start attempt={attempt} duration=0.000s"
                    if self._log_verbose:
                        logger.info(msg_s)
                    else:
                        logger.debug(msg_s)
                t0 = time.monotonic()
                self.last_run = await pipeline.run(
                    dataset=dataset, symbols=symbols, on_progress=on_progress, **run_kwargs
                )
                if self._structured_logs:
                    err_n = len(self.last_run.errors) if self.last_run else 0
                    dur = time.monotonic() - t0
                    attempt = self._safe_int(run_kwargs.get("attempt", 0))
                    msg_e = f"OBS provider={sanitize_field(router.provider)} dataset={sanitize_field(dataset)} symbol=* stage=client_run_end errors={err_n} attempt={attempt} duration={dur:.3f}s"
                    if self._log_verbose:
                        logger.info(msg_e)
                    else:
                        logger.debug(msg_e)
            return self.last_run

    @asynccontextmanager
    async def _http_client(self) -> AsyncGenerator[httpx.AsyncClient, None]:
        """Manage the HTTP client lifecycle.

        Yields the existing client if available, or creates a temporary one
        for the duration of the context.
        """
        if self._client is not None:
            yield self._client
            return

        client = default_async_client()
        try:
            self._client = client
            yield client
        finally:
            await client.aclose()
            if self._client is client:
                self._client = None

    @asynccontextmanager
    async def managed_writer(
        self,
        connect_db: str | Path | None,
        *,
        show_progress: bool = True,
    ) -> AsyncGenerator[BaseWriter, None]:
        """Manage writer lifecycle with proper resource cleanup.
        
        This is a common infrastructure component that all providers can use
        to ensure consistent writer lifecycle management.
        
        Args:
            connect_db: Database connection string/path, or None for in-memory.
            show_progress: Whether to show progress indicators (default: True).
            
        Yields:
            BaseWriter: Properly initialized writer instance.
        
        Raises:
            duckdb.Error: If a DuckDB connection cannot be established.
            ValidationError: If writer initialization fails due to schema issues.
            Exception: Any unexpected errors during writer setup are propagated.
        
        Example:
            async with self.managed_writer(connect_db, show_progress=True) as writer:
                result = await self.run_pipeline(..., writer=writer)
        """
        stack = AsyncExitStack()
        await stack.__aenter__()
        try:
            writer = await stack.enter_async_context(create_writer(connect_db))
            try:
                yield writer
            finally:
                run = self.last_run
                if run and run.errors:
                    for err in run.errors:
                        logger.error("%s", err)
        finally:
            with (Spinner("Finalizing database writes...") if show_progress else nullcontext()):
                await stack.__aexit__(None, None, None)

    def create_progress_tracker(
        self,
        *,
        total_items: int | None = None,
        unit: str = "it",
        desc: str = "Processing",
        show_progress: bool = True,
    ) -> tuple[Any, Callable | None]:
        """Create progress tracking infrastructure.
        
        Common progress tracking setup that can be used by all providers.
        Uses `tqdm` only when `show_progress=True`; otherwise returns (None, None)
        to minimize overhead for high-performance/headless runs.
        
        Args:
            total_items: Total number of items to process
            unit: Unit label (e.g., "tickers", "pages", "it")
            desc: Description for the progress bar
            show_progress: Whether to show progress bar. If False, tqdm is skipped.
            
        Returns:
            tuple: (progress_bar_object, progress_updater_callback)
            Both will be None if show_progress is False
        """
        if not show_progress:
            return None, None
            
        pbar = tqdm(
            total=total_items,
            unit=unit,
            desc=desc,
            leave=True,
            disable=False,
        )
        pbar_updater = create_pbar_updater(pbar)
        return pbar, pbar_updater

    async def collect_results(
        self,
        writer: BaseWriter,
        table_name: str,
        connect_db: str | Path | None,
        *,
        sort_by_unique_key: bool = True,
    ) -> pl.DataFrame | RunResult:
        """Collect and return results from writer.
        
        Common result collection logic that handles both database and in-memory scenarios.
        
        Args:
            writer: Writer instance to collect from
            table_name: Name of the table to collect
            connect_db: Database connection (determines collection mode)
            sort_by_unique_key: Whether to sort by schema's unique key if available
            
        Returns:
            pl.DataFrame if in-memory mode, RunResult if database mode
        """
        if connect_db is not None:
            # Database mode: return RunResult from pipeline
            if self.last_run is None:
                raise RuntimeError(
                    f"No pipeline result available for table '{table_name}'. "
                    "Ensure run_pipeline completed before collecting database results."
                )
            return self.last_run
            
        # In-memory mode: collect DataFrame from writer
        sort_cols = None
        if sort_by_unique_key:
            # Use schema unique_key for deterministic sorting if available
            schema = get_table_schema(table_name)
            if schema and schema.unique_key:
                sort_cols = list(schema.unique_key)
                
        return writer.collect_table(table_name, sort_cols=sort_cols)
