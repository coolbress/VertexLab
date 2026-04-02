from __future__ import annotations

from abc import ABC
import asyncio
from collections.abc import Mapping
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
from dataclasses import dataclass
from functools import partial
import logging
import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from vertex_forager.constants import (
    FLUSH_THRESHOLD_ROWS,
    HTTP_TIMEOUT_S,
    MEM_THRESHOLD_ABS_MB,
    MEM_THRESHOLD_RATIO,
)
from vertex_forager.core.config import (
    AdaptiveThrottleConfig,
    AdvancedConfig,
    HTTPConfig,
    ProgressSnapshot,
    ResolvedClientConfig,
    RetryConfig,
    RunResult,
    SchedulerConfig,
)
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor as _HttpExecutor
from vertex_forager.core.http import build_async_client
from vertex_forager.core.http import default_async_client as _default_async_client
from vertex_forager.core.pipeline import VertexForager as _VertexForager
from vertex_forager.core.types import JSONValue, SharadarDataset, YFinanceDataset
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.utils import (
    Spinner,
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
    import polars as pl

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


def default_async_client() -> httpx.AsyncClient:
    return _default_async_client()


def _parse_flag(value: Any, default: bool) -> Any:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return value


@dataclass
class _NormalizedClientSettings:
    runtime_config: ResolvedClientConfig
    http_timeout_s: float
    http_limits: HTTPConfig


def _normalize_client_settings(
    *,
    rate_limit: int,
    structured_logs: bool | None,
    log_verbose: bool | None,
    schedule: SchedulerConfig | dict[str, Any] | None,
    retry: RetryConfig | dict[str, Any] | None,
    adaptive_throttle: AdaptiveThrottleConfig | dict[str, Any] | None,
    concurrency: int | None,
    flush_threshold_rows: int | None,
    checkpoint_retention_days: int | None,
    run_history_retention_days: int | None,
    http_timeout_s: float | None,
    limits: HTTPConfig | dict[str, Any] | None,
    advanced: AdvancedConfig | dict[str, Any] | None,
) -> _NormalizedClientSettings:
    adaptive_throttle_config = (
        _coerce_grouped_config(adaptive_throttle, AdaptiveThrottleConfig) or AdaptiveThrottleConfig()
    )
    schedule_config = _coerce_grouped_config(schedule, SchedulerConfig) or SchedulerConfig()
    limits_config = _coerce_grouped_config(limits, HTTPConfig) or HTTPConfig()
    advanced_config = _coerce_grouped_config(advanced, AdvancedConfig) or AdvancedConfig()
    retry_config = _coerce_grouped_config(retry, RetryConfig) or RetryConfig()

    runtime_config = ResolvedClientConfig(
        requests_per_minute=rate_limit,
        structured_logs=_parse_flag(structured_logs, False),
        log_verbose=_parse_flag(log_verbose, False),
        schedule=schedule_config,
        retry=retry_config,
        adaptive_throttle=adaptive_throttle_config,
        concurrency=concurrency,
        flush_threshold_rows=flush_threshold_rows if flush_threshold_rows is not None else FLUSH_THRESHOLD_ROWS,
        http_timeout_s=http_timeout_s if http_timeout_s is not None else HTTP_TIMEOUT_S,
        limits=limits_config,
        advanced=advanced_config,
        checkpoint_retention_days=checkpoint_retention_days if checkpoint_retention_days is not None else 7,
        run_history_retention_days=run_history_retention_days if run_history_retention_days is not None else 90,
    )
    runtime_config.assert_valid()

    return _NormalizedClientSettings(
        runtime_config=runtime_config,
        http_timeout_s=runtime_config.http_timeout_s,
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
        structured_logs: bool | None = None,
        log_verbose: bool | None = None,
        schedule: SchedulerConfig | dict[str, Any] | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        adaptive_throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        concurrency: int | None = None,
        flush_threshold_rows: int | None = None,
        checkpoint_retention_days: int | None = None,
        run_history_retention_days: int | None = None,
        http_timeout_s: float | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
        advanced: AdvancedConfig | dict[str, Any] | None = None,
    ) -> None:
        """Initialize the base client infrastructure.

        Args:
            api_key: API key for the provider (optional, depends on provider).
            rate_limit: Maximum requests per minute (RPM) allowed for this client.
            structured_logs: Enables structured stage logs when True.
            log_verbose: Promotes structured logs to INFO when True.
            schedule: Grouped scheduler configuration for always-on DRR fairness.
            retry: Grouped retry policy configuration.
            adaptive_throttle: Grouped adaptive throttle policy configuration.
            concurrency: Explicit fetch concurrency limit.
            flush_threshold_rows: Buffered row threshold before flush begins.
            checkpoint_retention_days: Retention window for completed checkpoints.
            run_history_retention_days: Retention window for run-history records.
            http_timeout_s: HTTP request timeout in seconds.
            limits: Grouped HTTP connection-pool configuration.
            advanced: Grouped advanced and transitional settings.
        """
        self.api_key = api_key
        normalized = _normalize_client_settings(
            rate_limit=rate_limit,
            structured_logs=structured_logs,
            log_verbose=log_verbose,
            schedule=schedule,
            retry=retry,
            adaptive_throttle=adaptive_throttle,
            concurrency=concurrency,
            flush_threshold_rows=flush_threshold_rows,
            checkpoint_retention_days=checkpoint_retention_days,
            run_history_retention_days=run_history_retention_days,
            http_timeout_s=http_timeout_s,
            limits=limits,
            advanced=advanced,
        )

        self._config = normalized.runtime_config
        self._structured_logs = bool(self._config.structured_logs)
        self._log_verbose = bool(self._config.log_verbose)
        self._http_timeout_s = normalized.http_timeout_s
        self._http_limits = normalized.http_limits

        self.controller = FlowController(
            requests_per_minute=self._config.requests_per_minute,
            concurrency_limit=self._config.fetch_concurrency,
            adaptive_throttle_enabled=self._config.adaptive_throttle_enabled,
            adaptive_throttle_window_s=self._config.adaptive_throttle_window_s,
            error_rate_threshold=self._config.error_rate_threshold,
            rpm_floor_ratio=self._config.rpm_floor_ratio,
            recovery_factor=self._config.recovery_factor,
            healthy_window_s=self._config.healthy_window_s,
        )
        self.last_run: RunResult | None = None
        self._client: httpx.AsyncClient | None = None

    def _build_http_client(self) -> httpx.AsyncClient:
        return build_async_client(
            timeout_s=self._http_timeout_s,
            max_keepalive_connections=self._http_limits.max_keepalive_connections,
            max_connections=self._http_limits.max_connections,
        )

    def validate_memory_usage(
        self,
        *,
        symbols: list[str] | None,
        connect_db: str | Path | None,
        bytes_per_item: int,
    ) -> None:
        validate_memory_usage_impl(
            symbols=symbols,
            connect_db=connect_db,
            bytes_per_item=bytes_per_item,
            threshold_ratio=MEM_THRESHOLD_RATIO,
            threshold_absolute=MEM_THRESHOLD_ABS_MB * 1024 * 1024,
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

        # Structured log: start
        if self._structured_logs:
            sym_count = len(symbols or [])
            attempt = self._safe_int(run_kwargs.get("attempt", 0))
            msg_s = (
                f"OBS provider={sanitize_field(router.provider)} "
                f"dataset={sanitize_field(dataset)} "
                f"symbol=* symbols={sym_count} "
                f"stage=client_run_start attempt={attempt} duration=0.000s"
            )
            if self._log_verbose:
                logger.info(msg_s)
            else:
                logger.debug(msg_s)

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

        # Structured log: end
        if self._structured_logs:
            err_n = len(self.last_run.errors) if self.last_run else 0
            dur = time.monotonic() - t0
            attempt = self._safe_int(run_kwargs.get("attempt", 0))
            msg_e = (
                f"OBS provider={sanitize_field(router.provider)} "
                f"dataset={sanitize_field(dataset)} "
                f"symbol=* stage=client_run_end errors={err_n} "
                f"attempt={attempt} duration={dur:.3f}s"
            )
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

        return df
