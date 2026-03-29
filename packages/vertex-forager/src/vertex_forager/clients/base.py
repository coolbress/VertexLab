from __future__ import annotations

from abc import ABC
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
from dataclasses import dataclass
from functools import partial
import logging
import os
import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar
import warnings

from tqdm.auto import tqdm

from vertex_forager.constants import FLUSH_THRESHOLD_ROWS, HTTP_TIMEOUT_S
from vertex_forager.core.config import (
    AdvancedConfig,
    DownshiftConfig,
    HTTPConfig,
    ResolvedClientConfig,
    RetryConfig,
    RunResult,
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
    create_pbar_updater,
    env_bool,
    env_float,
    env_int,
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


def _warn_deprecated(message: str) -> None:
    warnings.warn(message, DeprecationWarning, stacklevel=4)


def _coerce_grouped_config(value: Any, model_cls: type[Any]) -> Any:
    if value is None:
        return None
    if isinstance(value, model_cls):
        return value
    if isinstance(value, dict):
        return model_cls(**value)
    return model_cls.model_validate(value)


def _env_var_present(name: str) -> bool:
    return name in os.environ


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
    memory_threshold_ratio: float
    memory_threshold_absolute: int | None


def _apply_legacy_downshift_kwargs(
    downshift: DownshiftConfig,
    config_params: dict[str, Any],
) -> DownshiftConfig:
    legacy_downshift_map = {
        "enabled": "downshift_enabled",
        "window_s": "downshift_window_s",
        "error_rate_threshold": "error_rate_threshold",
        "rpm_floor": "rpm_floor",
        "recovery_step": "recovery_step",
        "healthy_window_s": "healthy_window_s",
    }
    legacy_values = {
        field: config_params.pop(legacy_key)
        for field, legacy_key in legacy_downshift_map.items()
        if legacy_key in config_params
    }
    if not legacy_values:
        return downshift
    _warn_deprecated("Flat downshift kwargs are deprecated; pass downshift=DownshiftConfig(...) instead.")
    merged = downshift.model_dump()
    merged.update(legacy_values)
    return DownshiftConfig(**merged)


def _apply_legacy_advanced_kwargs(
    advanced: AdvancedConfig,
    config_params: dict[str, Any],
) -> AdvancedConfig:
    legacy_keys = (
        "dlq_tmp_cleanup_on_error",
        "dlq_tmp_periodic_cleanup",
        "dlq_tmp_retention_s",
        "tracer",
        "otel_enabled",
        "mem_threshold_ratio",
        "mem_threshold_abs_mb",
    )
    legacy_values = {key: config_params.pop(key) for key in legacy_keys if key in config_params}
    if not legacy_values:
        return advanced
    _warn_deprecated("Flat advanced kwargs are deprecated; pass advanced=AdvancedConfig(...) instead.")
    merged = advanced.model_dump()
    merged.update(legacy_values)
    return AdvancedConfig(**merged)


def _resolve_env_behavior_backfills(
    *,
    metrics_enabled: bool | None,
    structured_logs: bool | None,
    log_verbose: bool | None,
    concurrency: int | None,
    flush_threshold_rows: int | None,
) -> tuple[bool | None, bool | None, bool | None, int | None, int | None]:
    if metrics_enabled is None:
        env_metrics = env_bool("VF_METRICS_ENABLED")
        if _env_var_present("VF_METRICS_ENABLED"):
            _warn_deprecated("VF_METRICS_ENABLED is deprecated; pass metrics_enabled=... instead.")
            metrics_enabled = env_metrics
    if structured_logs is None:
        env_structured_logs = env_bool("VF_STRUCTURED_LOGS")
        if _env_var_present("VF_STRUCTURED_LOGS"):
            _warn_deprecated("VF_STRUCTURED_LOGS is deprecated; pass structured_logs=... instead.")
            structured_logs = env_structured_logs
    if log_verbose is None:
        env_log_verbose = env_bool("VF_LOG_VERBOSE")
        if _env_var_present("VF_LOG_VERBOSE"):
            _warn_deprecated("VF_LOG_VERBOSE is deprecated; pass log_verbose=... instead.")
            log_verbose = env_log_verbose
    if concurrency is None:
        env_concurrency = env_int("VF_CONCURRENCY")
        if _env_var_present("VF_CONCURRENCY") and env_concurrency is not None and env_concurrency > 0:
            _warn_deprecated("VF_CONCURRENCY is deprecated; pass concurrency=... instead.")
            concurrency = env_concurrency
    if flush_threshold_rows is None:
        env_flush_threshold_rows = env_int("VF_FLUSH_THRESHOLD_ROWS")
        if (
            _env_var_present("VF_FLUSH_THRESHOLD_ROWS")
            and env_flush_threshold_rows is not None
            and env_flush_threshold_rows > 0
        ):
            _warn_deprecated("VF_FLUSH_THRESHOLD_ROWS is deprecated; pass flush_threshold_rows=... instead.")
            flush_threshold_rows = env_flush_threshold_rows
    return metrics_enabled, structured_logs, log_verbose, concurrency, flush_threshold_rows


def _resolve_env_transport_backfills(
    *,
    http_timeout_s: float | None,
    limits: HTTPConfig | dict[str, Any] | None,
    limits_config: HTTPConfig,
) -> tuple[float | None, HTTPConfig]:
    if http_timeout_s is None:
        env_http_timeout_s = env_float("VF_HTTP_TIMEOUT_S")
        if env_http_timeout_s is not None and env_http_timeout_s > 0:
            _warn_deprecated("VF_HTTP_TIMEOUT_S is deprecated; pass http_timeout_s=... instead.")
            http_timeout_s = env_http_timeout_s
    env_max_keepalive = env_int("VF_HTTP_MAX_KEEPALIVE")
    env_max_connections = env_int("VF_HTTP_MAX_CONNECTIONS")
    if limits is None and (
        (env_max_keepalive is not None and env_max_keepalive > 0)
        or (env_max_connections is not None and env_max_connections > 0)
    ):
        _warn_deprecated(
            "VF_HTTP_MAX_KEEPALIVE and VF_HTTP_MAX_CONNECTIONS are deprecated; pass limits=HTTPConfig(...) instead."
        )
        limits_config = limits_config.model_copy(
            update={
                "max_keepalive_connections": env_max_keepalive
                if env_max_keepalive is not None and env_max_keepalive > 0
                else limits_config.max_keepalive_connections,
                "max_connections": env_max_connections
                if env_max_connections is not None and env_max_connections > 0
                else limits_config.max_connections,
            }
        )
    return http_timeout_s, limits_config


def _resolve_env_advanced_backfills(
    *,
    advanced: AdvancedConfig | dict[str, Any] | None,
    advanced_config: AdvancedConfig,
) -> AdvancedConfig:
    if advanced is None and advanced_config.otel_enabled is None:
        env_otel_enabled = env_bool("VF_OTEL_ENABLED")
        if _env_var_present("VF_OTEL_ENABLED"):
            _warn_deprecated("VF_OTEL_ENABLED is deprecated; pass advanced=AdvancedConfig(otel_enabled=...) instead.")
            advanced_config = advanced_config.model_copy(update={"otel_enabled": env_otel_enabled})
    if advanced is None:
        env_mem_threshold_ratio = env_float("VF_MEM_THRESHOLD_RATIO")
        if env_mem_threshold_ratio is not None and 0 < env_mem_threshold_ratio <= 1:
            _warn_deprecated(
                "VF_MEM_THRESHOLD_RATIO is deprecated; pass advanced=AdvancedConfig(mem_threshold_ratio=...) instead."
            )
            advanced_config = advanced_config.model_copy(update={"mem_threshold_ratio": env_mem_threshold_ratio})
        env_mem_threshold_abs_mb = env_int("VF_MEM_THRESHOLD_ABS_MB")
        if env_mem_threshold_abs_mb is not None and env_mem_threshold_abs_mb > 0:
            _warn_deprecated(
                "VF_MEM_THRESHOLD_ABS_MB is deprecated; pass advanced=AdvancedConfig(mem_threshold_abs_mb=...) instead."
            )
            advanced_config = advanced_config.model_copy(update={"mem_threshold_abs_mb": env_mem_threshold_abs_mb})
    return advanced_config


def _resolve_env_backfills(
    *,
    metrics_enabled: bool | None,
    structured_logs: bool | None,
    log_verbose: bool | None,
    concurrency: int | None,
    flush_threshold_rows: int | None,
    http_timeout_s: float | None,
    limits: HTTPConfig | dict[str, Any] | None,
    advanced: AdvancedConfig | dict[str, Any] | None,
    advanced_config: AdvancedConfig,
    limits_config: HTTPConfig,
) -> tuple[bool | None, bool | None, bool | None, int | None, int | None, float | None, HTTPConfig, AdvancedConfig]:
    (
        metrics_enabled,
        structured_logs,
        log_verbose,
        concurrency,
        flush_threshold_rows,
    ) = _resolve_env_behavior_backfills(
        metrics_enabled=metrics_enabled,
        structured_logs=structured_logs,
        log_verbose=log_verbose,
        concurrency=concurrency,
        flush_threshold_rows=flush_threshold_rows,
    )
    http_timeout_s, limits_config = _resolve_env_transport_backfills(
        http_timeout_s=http_timeout_s,
        limits=limits,
        limits_config=limits_config,
    )
    advanced_config = _resolve_env_advanced_backfills(
        advanced=advanced,
        advanced_config=advanced_config,
    )
    return (
        metrics_enabled,
        structured_logs,
        log_verbose,
        concurrency,
        flush_threshold_rows,
        http_timeout_s,
        limits_config,
        advanced_config,
    )


def _normalize_client_settings(
    *,
    rate_limit: int,
    metrics_enabled: bool | None,
    structured_logs: bool | None,
    log_verbose: bool | None,
    dlq_enabled: bool | None,
    pagination_max_burst: int | None,
    retry: RetryConfig | dict[str, Any] | None,
    downshift: DownshiftConfig | dict[str, Any] | None,
    concurrency: int | None,
    flush_threshold_rows: int | None,
    writer_chunk_rows: int | None,
    writer_concurrency: int | None,
    persist_run_history: bool | None,
    http_timeout_s: float | None,
    limits: HTTPConfig | dict[str, Any] | None,
    advanced: AdvancedConfig | dict[str, Any] | None,
    kwargs: dict[str, Any],
) -> _NormalizedClientSettings:
    config_params = kwargs.copy()
    downshift_config = _apply_legacy_downshift_kwargs(
        _coerce_grouped_config(downshift, DownshiftConfig) or DownshiftConfig(),
        config_params,
    )
    limits_config = _coerce_grouped_config(limits, HTTPConfig) or HTTPConfig()
    advanced_config = _apply_legacy_advanced_kwargs(
        _coerce_grouped_config(advanced, AdvancedConfig) or AdvancedConfig(),
        config_params,
    )
    retry_config = _coerce_grouped_config(retry, RetryConfig) or RetryConfig()

    legacy_persist_run_history = config_params.pop("persist_run_history", None)
    if legacy_persist_run_history is not None:
        _warn_deprecated(
            "persist_run_history is deprecated and scheduled for removal; "
            "it remains supported only as a compatibility kwarg."
        )
        if persist_run_history is None:
            persist_run_history = legacy_persist_run_history

    legacy_writer_chunk_rows = config_params.pop("writer_chunk_rows", None)
    legacy_writer_concurrency = config_params.pop("writer_concurrency", None)
    if legacy_writer_chunk_rows is not None or legacy_writer_concurrency is not None:
        _warn_deprecated(
            "writer_chunk_rows and writer_concurrency are deprecated flat client kwargs "
            "and remain supported only for compatibility."
        )
        if writer_chunk_rows is None:
            writer_chunk_rows = legacy_writer_chunk_rows
        if writer_concurrency is None:
            writer_concurrency = legacy_writer_concurrency

    (
        metrics_enabled,
        structured_logs,
        log_verbose,
        concurrency,
        flush_threshold_rows,
        http_timeout_s,
        limits_config,
        advanced_config,
    ) = _resolve_env_backfills(
        metrics_enabled=metrics_enabled,
        structured_logs=structured_logs,
        log_verbose=log_verbose,
        concurrency=concurrency,
        flush_threshold_rows=flush_threshold_rows,
        http_timeout_s=http_timeout_s,
        limits=limits,
        advanced=advanced,
        advanced_config=advanced_config,
        limits_config=limits_config,
    )

    runtime_config = ResolvedClientConfig(
        requests_per_minute=rate_limit,
        metrics_enabled=_parse_flag(metrics_enabled, False),
        structured_logs=_parse_flag(structured_logs, False),
        log_verbose=_parse_flag(log_verbose, False),
        dlq_enabled=_parse_flag(dlq_enabled, True),
        pagination_max_burst=pagination_max_burst,
        retry=retry_config,
        downshift=downshift_config,
        concurrency=concurrency,
        flush_threshold_rows=flush_threshold_rows if flush_threshold_rows is not None else FLUSH_THRESHOLD_ROWS,
        writer_chunk_rows=writer_chunk_rows,
        writer_concurrency=writer_concurrency if writer_concurrency is not None else 1,
        http_timeout_s=http_timeout_s if http_timeout_s is not None else HTTP_TIMEOUT_S,
        limits=limits_config,
        advanced=advanced_config,
        persist_run_history=_parse_flag(persist_run_history, True),
    )
    runtime_config.assert_valid()

    return _NormalizedClientSettings(
        runtime_config=runtime_config,
        http_timeout_s=runtime_config.http_timeout_s,
        http_limits=runtime_config.limits,
        memory_threshold_ratio=runtime_config.advanced.mem_threshold_ratio,
        memory_threshold_absolute=None
        if runtime_config.advanced.mem_threshold_abs_mb is None
        else runtime_config.advanced.mem_threshold_abs_mb * 1024 * 1024,
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
        metrics_enabled: bool | None = None,
        structured_logs: bool | None = None,
        log_verbose: bool | None = None,
        dlq_enabled: bool | None = None,
        pagination_max_burst: int | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        downshift: DownshiftConfig | dict[str, Any] | None = None,
        concurrency: int | None = None,
        flush_threshold_rows: int | None = None,
        writer_chunk_rows: int | None = None,
        writer_concurrency: int | None = None,
        persist_run_history: bool | None = None,
        http_timeout_s: float | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
        advanced: AdvancedConfig | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the base client infrastructure.

        Args:
            api_key: API key for the provider (optional, depends on provider).
            rate_limit: Maximum requests per minute (RPM) allowed for this client.
            metrics_enabled: Enables metrics emission when True.
            structured_logs: Enables structured stage logs when True.
            log_verbose: Promotes structured logs to INFO when True.
            dlq_enabled: Enables DLQ spooling on persistence failures.
            pagination_max_burst: Pagination fairness burst cap.
            retry: Grouped retry policy configuration.
            downshift: Grouped adaptive downshift policy configuration.
            concurrency: Explicit fetch concurrency limit.
            flush_threshold_rows: Buffered row threshold before flush begins.
            writer_chunk_rows: Transitional write chunk-size tuning.
            writer_concurrency: Transitional writer worker count tuning.
            persist_run_history: Transitional run-history persistence toggle.
            http_timeout_s: HTTP request timeout in seconds.
            limits: Grouped HTTP connection-pool configuration.
            advanced: Grouped advanced and transitional settings.
            **kwargs: Legacy compatibility kwargs still normalized into internal config.
        """
        self.api_key = api_key
        normalized = _normalize_client_settings(
            rate_limit=rate_limit,
            metrics_enabled=metrics_enabled,
            structured_logs=structured_logs,
            log_verbose=log_verbose,
            dlq_enabled=dlq_enabled,
            pagination_max_burst=pagination_max_burst,
            retry=retry,
            downshift=downshift,
            concurrency=concurrency,
            flush_threshold_rows=flush_threshold_rows,
            writer_chunk_rows=writer_chunk_rows,
            writer_concurrency=writer_concurrency,
            persist_run_history=persist_run_history,
            http_timeout_s=http_timeout_s,
            limits=limits,
            advanced=advanced,
            kwargs=kwargs,
        )

        self._config = normalized.runtime_config
        self._structured_logs = bool(self._config.structured_logs)
        self._log_verbose = bool(self._config.log_verbose)
        self._http_timeout_s = normalized.http_timeout_s
        self._http_limits = normalized.http_limits
        self._memory_threshold_ratio = normalized.memory_threshold_ratio
        self._memory_threshold_absolute = normalized.memory_threshold_absolute

        self.controller = FlowController(
            requests_per_minute=self._config.requests_per_minute,
            concurrency_limit=self._config.fetch_concurrency,
            downshift_enabled=self._config.downshift.enabled,
            downshift_window_s=self._config.downshift.window_s,
            error_rate_threshold=self._config.downshift.error_rate_threshold,
            rpm_floor=self._config.downshift.rpm_floor,
            recovery_step=self._config.downshift.recovery_step,
            healthy_window_s=self._config.downshift.healthy_window_s,
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
            threshold_ratio=self._memory_threshold_ratio,
            threshold_absolute=self._memory_threshold_absolute,
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
        from vertex_forager.clients.validation import filter_reserved_kwargs

        reserved = {
            "router",
            "dataset",
            "symbols",
            "writer",
            "mapper",
            "on_progress",
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
        pbar_updater: Callable[..., None] = create_pbar_updater(pbar)
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

        # Merge any post-collection counters (e.g., in-memory dedup) into RunResult when metrics enabled
        if getattr(self._config, "metrics_enabled", False):
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
