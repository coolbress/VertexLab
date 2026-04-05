from __future__ import annotations

from abc import ABC
import asyncio
from collections.abc import Coroutine, Mapping
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
from dataclasses import dataclass
from functools import partial
import logging
import time
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from vertex_forager.constants import (
    MEM_THRESHOLD_ABS_MB,
    MEM_THRESHOLD_RATIO,
)
from vertex_forager.core.config import (
    AdaptiveThrottleConfig,
    HTTPConfig,
    ProgressSnapshot,
    ResolvedClientConfig,
    RetryConfig,
    RunResult,
    SchedulerConfig,
    StorageConfig,
)
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor as _HttpExecutor
from vertex_forager.core.http import build_async_client
from vertex_forager.core.pipeline import VertexForager as _VertexForager
from vertex_forager.core.types import JSONValue, SharadarDataset, YFinanceDataset
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.utils import (
    Spinner,
    run_sync_compat,
    sanitize_field,
)
from vertex_forager.utils import (
    validate_memory_usage as validate_memory_usage_impl,
)
from vertex_forager.writers import create_writer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from pathlib import Path
    from types import TracebackType

    import httpx

    from vertex_forager.core.contracts import IMapper, IRouter, IWriter
    from vertex_forager.writers.base import BaseWriter

HttpExecutor = _HttpExecutor
VertexForager = _VertexForager


logger = logging.getLogger(__name__)

T = TypeVar("T", bound=SharadarDataset | YFinanceDataset | str)


def _coerce_grouped_config(value: Any, model_cls: type[Any]) -> Any:
    if value is None:
        return None
    if isinstance(value, model_cls):
        return value
    if isinstance(value, Mapping):
        return model_cls(**value)
    return model_cls.model_validate(value)


@dataclass
class _NormalizedClientSettings:
    runtime_config: ResolvedClientConfig
    http_limits: HTTPConfig


def _normalize_client_settings(
    *,
    rate_limit: int,
    schedule: SchedulerConfig | dict[str, Any] | None,
    retry: RetryConfig | dict[str, Any] | None,
    throttle: AdaptiveThrottleConfig | dict[str, Any] | None,
    quality_check: Literal["warn", "error"],
    concurrency: int | None,
    storage: StorageConfig | dict[str, Any] | None,
    limits: HTTPConfig | dict[str, Any] | None,
) -> _NormalizedClientSettings:
    throttle_config = _coerce_grouped_config(throttle, AdaptiveThrottleConfig) or AdaptiveThrottleConfig()
    schedule_config = _coerce_grouped_config(schedule, SchedulerConfig) or SchedulerConfig()
    limits_config = _coerce_grouped_config(limits, HTTPConfig) or HTTPConfig()
    retry_config = _coerce_grouped_config(retry, RetryConfig) or RetryConfig()
    storage_config = _coerce_grouped_config(storage, StorageConfig) or StorageConfig()

    runtime_config = ResolvedClientConfig(
        requests_per_minute=rate_limit,
        schedule=schedule_config,
        retry=retry_config,
        throttle=throttle_config,
        quality_check=quality_check,
        concurrency=concurrency,
        storage=storage_config,
        limits=limits_config,
    )
    runtime_config.assert_valid()

    return _NormalizedClientSettings(
        runtime_config=runtime_config,
        http_limits=runtime_config.limits,
    )


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
    4. **Configuration**: Centralizes grouped runtime configuration handling.

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
        schedule: SchedulerConfig | dict[str, Any] | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        quality_check: Literal["warn", "error"] = "warn",
        concurrency: int | None = None,
        storage: StorageConfig | dict[str, Any] | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
    ) -> None:
        """Initialize the base client infrastructure.

        Args:
            api_key: API key for the provider (optional, depends on provider).
            rate_limit: Maximum requests per minute (RPM) allowed for this client.
            schedule: Grouped scheduler configuration for always-on DRR fairness.
            retry: Grouped retry policy configuration.
            throttle: Grouped adaptive throttle policy configuration.
            quality_check: Data quality violation handling mode.
            concurrency: Explicit fetch concurrency limit.
            storage: Grouped data-lifecycle and write-path tuning settings.
            limits: Grouped HTTP connection-pool configuration.
        """
        self.api_key = api_key
        normalized = _normalize_client_settings(
            rate_limit=rate_limit,
            schedule=schedule,
            retry=retry,
            throttle=throttle,
            quality_check=quality_check,
            concurrency=concurrency,
            storage=storage,
            limits=limits,
        )

        self._config = normalized.runtime_config
        self._http_limits = normalized.http_limits

        self.controller = FlowController(
            requests_per_minute=self._config.requests_per_minute,
            concurrency_limit=self._config.concurrency,
            adaptive_throttle_window_s=self._config.throttle.window_s,
            error_rate_threshold=self._config.throttle.error_rate_threshold,
            rpm_floor_ratio=self._config.throttle.rpm_floor_ratio,
            recovery_factor=self._config.throttle.recovery_factor,
            healthy_window_s=self._config.throttle.healthy_window_s,
        )
        self._client: httpx.AsyncClient | None = None
        self.last_run: RunResult | None = None

    def _run_sync_compat(self, coro: Coroutine[Any, Any, Any]) -> Any:
        return run_sync_compat(coro)

    def _build_http_client(self) -> httpx.AsyncClient:
        return build_async_client(
            timeout_s=self._http_limits.timeout_s,
            max_keepalive_connections=self._http_limits.max_keepalive_connections,
            max_connections=self._http_limits.max_connections,
        )

    def validate_memory_usage(
        self,
        *,
        symbols: list[str] | None,
        connect_db: str | Path | None,
        bytes_per_item: int,
        estimated_count: int | None = None,
    ) -> None:
        validate_memory_usage_impl(
            symbols=symbols,
            connect_db=connect_db,
            bytes_per_item=bytes_per_item,
            threshold_ratio=MEM_THRESHOLD_RATIO,
            threshold_absolute=MEM_THRESHOLD_ABS_MB * 1024 * 1024,
            estimated_count=estimated_count,
        )

    async def aclose(self) -> None:
        """Asynchronously close the underlying HTTP client to release resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> BaseClient:
        """Async context manager entry.

        Initializes the shared HTTP client.
        """
        if self._client is None:
            self._client = self._build_http_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Async context manager exit.

        Ensures the HTTP client is closed.
        """
        await self.aclose()

    @property
    def config(self) -> ResolvedClientConfig:
        """Resolved internal configuration snapshot.

        Returns:
            ResolvedClientConfig: The configuration object governing rate limits,
                concurrency, queue sizes, and thresholds used by this client.

        Notes:
            Read-only accessor. Callers should not mutate the returned object
            in place. Public callers should prefer `create_client(...)` and
            grouped config inputs rather than constructing this model directly.
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
        on_progress: Callable[[ProgressSnapshot], None] | None = None,
        progress: bool = False,
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
            on_progress: Optional callback receiving ProgressSnapshot per completed request.
            progress: Whether to show built-in progress output and final summary.
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
        from vertex_forager.clients.validation import filter_reserved_kwargs

        reserved = {
            "router",
            "dataset",
            "symbols",
            "writer",
            "mapper",
            "on_progress",
            "progress",
            "http_executor_cls",
            "vertex_forager_cls",
        }
        run_kwargs = filter_reserved_kwargs(kwargs, reserved)

        sym_count = len(symbols or [])
        attempt = self._safe_int(run_kwargs.get("attempt", 0))
        logger.debug(
            "vertex_forager stage",
            extra={
                "vf_provider": sanitize_field(router.provider),
                "vf_dataset": sanitize_field(dataset),
                "vf_symbol": "*",
                "vf_stage": "client_run_start",
                "vf_symbols": sym_count,
                "vf_attempt": attempt,
                "vf_duration_s": 0.0,
            },
        )

        # Delegate orchestration to dispatcher
        from vertex_forager.clients.dispatcher import run_pipeline_for

        t0 = time.monotonic()
        self.last_run = await run_pipeline_for(
            client=self,
            router=router,
            dataset=dataset,
            symbols=symbols,
            writer=writer,
            mapper=mapper,
            on_progress=on_progress,
            progress=progress,
            http_executor_cls=HttpExecutor,
            vertex_forager_cls=VertexForager,
            **run_kwargs,
        )

        err_n = len(self.last_run.errors) if self.last_run else 0
        dur = round(time.monotonic() - t0, 3)
        attempt = self._safe_int(run_kwargs.get("attempt", 0))
        logger.debug(
            "vertex_forager stage",
            extra={
                "vf_provider": sanitize_field(router.provider),
                "vf_dataset": sanitize_field(dataset),
                "vf_symbol": "*",
                "vf_stage": "client_run_end",
                "vf_errors": err_n,
                "vf_attempt": attempt,
                "vf_duration_s": dur,
            },
        )
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

        client = self._build_http_client()
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
            with Spinner("Finalizing database writes...") if show_progress else nullcontext():
                await stack.__aexit__(None, None, None)

    async def collect_results(
        self,
        writer: BaseWriter,
        table_name: str,
        connect_db: str | Path | None,
        *,
        sort_by_unique_key: bool = True,
    ) -> RunResult:
        """Collect and return results from writer.

        Common result collection logic that handles both database and in-memory scenarios.

        Args:
            writer: Writer instance to collect from
            table_name: Name of the table to collect
            connect_db: Database connection (determines collection mode)
            sort_by_unique_key: Whether to sort by schema's unique key if available

        Returns:
            RunResult for both in-memory and database modes
        """
        if self.last_run is None:
            raise RuntimeError(
                f"No pipeline result available for table '{table_name}'. "
                "Ensure run_pipeline completed before collecting results."
            )
        run_result = self.last_run
        if connect_db is not None:
            run_result.data = None
            return run_result

        # In-memory mode: collect DataFrame from writer
        sort_cols = None
        schema = get_table_schema(table_name)
        # Always attempt unique_key setup for in-memory writer if schema has one
        if schema and schema.unique_key:
            try:
                # local import for optionality
                from vertex_forager.writers.memory import InMemoryBufferWriter as _IMW

                if isinstance(writer, _IMW):
                    writer.set_unique_key(list(schema.unique_key))
            except Exception as e:
                msg = (
                    "InMemoryBufferWriter unique_key setup failed "
                    "(independent of sorting): writer=%s unique_key=%s error=%s"
                )
                logger.debug(msg, writer, list(schema.unique_key), e)
            if sort_by_unique_key:
                sort_cols = list(schema.unique_key)

        df = writer.collect_table(table_name, sort_cols=sort_cols)

        try:
            counters = getattr(writer, "get_counters_and_reset", None)
            if callable(counters):
                writer_counts = dict(counters())
                if self.last_run is not None:
                    base = dict(self.last_run.metrics_counters or {})
                    for k, v in writer_counts.items():
                        base[k] = base.get(k, 0) + int(v)
                    self.last_run.metrics_counters = base
        except Exception as e:
            logger.debug("Merging writer counters after collect_table failed: error=%s", e)

        run_result.data = df
        return run_result
