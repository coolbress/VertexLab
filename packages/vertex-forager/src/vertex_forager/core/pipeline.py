"""Core Pipeline Engine Module.

This module implements the central `VertexForager` engine which orchestrates the
asynchronous data collection pipeline. It manages producer tasks (job generation),
worker tasks (fetching and parsing), and writer tasks (persisting data).

Classes:
    VertexForager: The main engine class coordinating the pipeline.

Usage:
        engine = VertexForager(
            router=sharadar_router,
            http=http_executor,
            writer=duckdb_writer,
            mapper=schema_mapper,
            config=engine_config,
            controller=flow_controller,
        )
        result = await engine.run(dataset="sep", symbols=["AAPL", "MSFT"])
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
import inspect
import itertools
import logging
import os
import time
from typing import TYPE_CHECKING, Any, Literal, cast

import httpx
from polars.exceptions import ComputeError

from vertex_forager.constants import (
    FLUSH_THRESHOLD_INFINITE as CONST_FLUSH_THRESHOLD_INFINITE,
)
from vertex_forager.constants import (
    FLUSH_THRESHOLD_ROWS as DEFAULT_FLUSH_THRESHOLD_ROWS,
)
from vertex_forager.constants import (
    PRIORITY_NEW_JOB as CONST_PRIORITY_NEW_JOB,
)
from vertex_forager.constants import (
    PRIORITY_PAGINATION as CONST_PRIORITY_PAGINATION,
)
from vertex_forager.constants import (
    PRIORITY_SENTINEL as CONST_PRIORITY_SENTINEL,
)
from vertex_forager.constants import (
    PROGRESS_LOG_CHUNK_ROWS,
)
from vertex_forager.core.checkpoint import (
    Checkpoint,
    find_latest_checkpoint,
    get_cache_dir,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.dlq import (
    build_writer_error_summary,
    spool_to_dlq_and_rescue,
)
from vertex_forager.core.lifecycle import (
    RunFinalizer,
)
from vertex_forager.core.lifecycle import (
    create_run_queues as create_run_queues_impl,
)
from vertex_forager.core.lifecycle import (
    create_run_result as create_run_result_impl,
)
from vertex_forager.core.lifecycle import (
    init_metrics_for_run as init_metrics_for_run_impl,
)
from vertex_forager.core.lifecycle import (
    initialize_run_state as initialize_run_state_impl,
)
from vertex_forager.core.orchestration import (
    await_packet_sentinel_tasks as await_packet_sentinel_tasks_impl,
)
from vertex_forager.core.orchestration import (
    await_writer_tasks as await_writer_tasks_impl,
)
from vertex_forager.core.orchestration import (
    cancel_non_writer_tasks as cancel_non_writer_tasks_impl,
)
from vertex_forager.core.orchestration import (
    enqueue_request_sentinels as enqueue_request_sentinels_impl,
)
from vertex_forager.core.orchestration import (
    schedule_packet_sentinels as schedule_packet_sentinels_impl,
)
from vertex_forager.core.quality import (
    validate_data_quality as validate_data_quality_impl,
)
from vertex_forager.core.retry import (
    RetryExecutor,
)
from vertex_forager.core.scheduler import (
    FairnessState,
    SchedulerResult,
)
from vertex_forager.core.scheduler import (
    pop_next_job_respecting_fairness as pop_next_job_respecting_fairness_impl,
)
from vertex_forager.core.workerio import (
    emit_packets_and_next_jobs as emit_packets_and_next_jobs_impl,
)
from vertex_forager.core.workerio import (
    fetch_payload as fetch_payload_impl,
)
from vertex_forager.core.workerio import (
    parse_payload as parse_payload_impl,
)
from vertex_forager.core.workerio import (
    record_worker_error as record_worker_error_impl,
)
from vertex_forager.core.writerflush import (
    WriterContext,
    buffer_or_flush_packet,
    concat_frames_with_flex,
    flush_all_writer_buffers,
    flush_chunked_table,
    flush_legacy_table,
    flush_on_writer_cancel,
    flush_writer_table,
    handle_writer_flush_error,
    validate_unique_key,
    write_merged_packet,
    writer_worker,
)

try:
    import duckdb as _duckdb
except ImportError:
    _duckdb = cast("Any", None)

from vertex_forager.core.config import (
    FetchJob,
    FramePacket,
    ParseResult,
    ResolvedClientConfig,
    RunResult,
)
from vertex_forager.core.errors import (
    DLQSpoolError,
    FetchError,
    PrimaryKeyMissingError,
    PrimaryKeyNullError,
    RunError,
    ValidationError,
)
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.utils import sanitize_field

if TYPE_CHECKING:
    import polars as pl

    from vertex_forager.core.contracts import IMapper, IRouter, IWriter
    from vertex_forager.core.controller import FlowController
    from vertex_forager.core.http import HttpExecutor

InMemoryBufferWriterType: type | None
try:
    import vertex_forager.writers.memory as _mem_writer

    InMemoryBufferWriterType = _mem_writer.InMemoryBufferWriter
except (ImportError, ModuleNotFoundError):
    InMemoryBufferWriterType = None

DuckDBWriterType: type | None
try:
    import vertex_forager.writers.duckdb as _duckdb_writer

    DuckDBWriterType = _duckdb_writer.DuckDBWriter
except (ImportError, ModuleNotFoundError):
    DuckDBWriterType = None

logger = logging.getLogger("vertex_forager.debug")

Symbols = Sequence[str]


class VertexForager:
    """High-performance asynchronous data pipeline engine.

    This class orchestrates the end-to-end data collection process using a
    Producer-Consumer architecture with asyncio. It manages three main stages:
    Job Generation (Producer), Data Fetching (Fetch Workers), and Data Writing
    (Writer Workers).

    Attributes:
        _router (IRouter): Router/Queue manager for fetch jobs.
        _http (HttpExecutor): Handles HTTP requests.
        _writer (IWriter): Writer task manager.
        _mapper (IMapper): Normalizes data schemas.
        _config (ResolvedClientConfig): Internal resolved configuration object.
        controller (FlowController): Rate limiter and concurrency controller.
        _flush_threshold (int): Row count threshold for flushing buffers.

    Public Methods:
        run(dataset: str, symbols: Symbols | None, ...) -> RunResult:
            Execute the full collection pipeline.
        stop() -> None:
            Gracefully stop the pipeline.
    """

    # Configurable flush threshold
    # Increased to 500k to allow better batching for large packets (125k rows each)
    FLUSH_THRESHOLD_ROWS = DEFAULT_FLUSH_THRESHOLD_ROWS

    # Priority Constants
    PRIORITY_PAGINATION = CONST_PRIORITY_PAGINATION
    PRIORITY_NEW_JOB = CONST_PRIORITY_NEW_JOB
    PRIORITY_SENTINEL = CONST_PRIORITY_SENTINEL

    # Infinite threshold for in-memory writer
    FLUSH_THRESHOLD_INFINITE = CONST_FLUSH_THRESHOLD_INFINITE
    _MAX_HIST_SAMPLES = 1024

    def __init__(
        self,
        *,
        router: IRouter,
        http: HttpExecutor,
        writer: IWriter,
        mapper: IMapper,
        config: ResolvedClientConfig,
        controller: FlowController,
    ) -> None:
        self._router = router
        self._http = http
        self._writer = writer
        self._mapper = mapper
        self._config = config
        self.controller = controller
        self._metrics_enabled = bool(config.metrics_enabled)
        self._structured_logs = bool(config.structured_logs)
        self._log_verbose = bool(config.log_verbose)
        self._counters: dict[str, int] = {}
        self._hists: dict[str, deque[float]] = {}
        self._tracer = config.advanced.tracer
        self._otel_enabled = bool(config.advanced.otel_enabled)

        # Track active tasks for graceful shutdown
        self._active_tasks: list[asyncio.Future[Any]] = []
        self._running = False

        # Validate configuration
        self._config.assert_valid()

        # Optimization: Disable intermediate flushing for In-Memory Writer
        # Since InMemoryBufferWriter just stores frames in a list, we can
        # avoid the overhead of intermediate merges by setting threshold to infinity.
        # This allows the worker to collect ALL frames and perform a SINGLE merge at the end.
        self._flush_threshold = config.flush_threshold_rows

        if InMemoryBufferWriterType is not None and isinstance(writer, InMemoryBufferWriterType):
            # Override instance config (not the global config object)
            # We treat 1 billion rows as effectively infinite for memory buffer
            self._flush_threshold = self.FLUSH_THRESHOLD_INFINITE
            logger.debug("PIPELINE: Detected InMemoryBufferWriter. Disabled intermediate flushing.")
        if DuckDBWriterType is not None and isinstance(writer, DuckDBWriterType) and int(config.writer_concurrency) > 1:
            logger.warning(
                "PIPELINE: writer_concurrency=%s requested with DuckDBWriter; DuckDB writes remain serialized.",
                config.writer_concurrency,
            )

        workers = getattr(self.controller, "concurrency_limit", None)
        try:
            w_int = int(workers) if workers is not None else None
            if w_int is not None and w_int <= 0:
                w_int = None
        except (ValueError, TypeError):
            w_int = None
        self._parse_executor = ThreadPoolExecutor(
            max_workers=w_int,
            thread_name_prefix="vertex-forager:parse",
        )
        # Global pagination fairness bookkeeping
        self._fair_lock = asyncio.Lock()
        self._fair_state = FairnessState()
        # Writer flush idempotence
        self._writer_flushed: bool = False
        self._writer_flush_attempted: bool = False
        self._summary: dict[str, float] = {}
        self._metrics_sink = getattr(config, "metrics_sink", None)
        self._stop_task: asyncio.Task[None] | None = None
        self._retry_executor = RetryExecutor(
            retry_config=self._config.retry,
            controller=self.controller,
            http_fetch=self._http.fetch,
            observe=self._observe,
            log_structured=self._log_structured,
        )
        self._run_finalizer = RunFinalizer(
            mapper=self._mapper,
            writer=self._writer,
            inc=self._inc,
            structured_logs=self._structured_logs,
            log_verbose=self._log_verbose,
            provider=str(getattr(self._router, "provider", "")),
            sanitize_field=sanitize_field,
            logger=logger,
        )
        self._writer_context = WriterContext(
            writer=self._writer,
            config=self._config,
            logger=logger,
            inc=self._inc,
            observe=self._observe,
            log_structured=self._log_structured,
            span=self._span,
            validate_data_quality=self._validate_data_quality,
            get_table_schema=get_table_schema,
            spool_to_dlq_and_rescue=spool_to_dlq_and_rescue,
            build_writer_error_summary=build_writer_error_summary,
            compute_error_cls=ComputeError,
            validation_error_cls=ValidationError,
            dlq_spool_error_cls=DLQSpoolError,
            duckdb_module=_duckdb,
            progress_log_chunk_rows=PROGRESS_LOG_CHUNK_ROWS,
            router_flexible_schema=bool(getattr(self._router, "flexible_schema", False)),
        )

        # For checkpoint tracking
        self._completed_symbols: set[str] = set()
        self._failed_symbols: set[str] = set()
        self._pending_symbol_jobs: dict[str, list[FetchJob]] = {}
        self._checkpoint_lock = asyncio.Lock()

        if str(os.getenv("VF_ALLOW_PICKLE_COMPAT", "")).lower() in {"1", "true", "yes"}:
            logger.warning("PIPELINE: VF_ALLOW_PICKLE_COMPAT is enabled; pickled payloads may be accepted")
            self._inc("pickle_compat_enabled", 1)

    def _inc(self, name: str, amount: int = 1) -> None:
        if not self._metrics_enabled:
            return
        self._counters[name] = self._counters.get(name, 0) + amount
        sink = self._metrics_sink
        if sink is not None:
            with suppress(Exception):
                sink.inc(name, amount)

    def _observe(self, name: str, value: float) -> None:
        if not self._metrics_enabled:
            return
        bucket = self._hists.get(name)
        if bucket is None:
            bucket = deque(maxlen=self._MAX_HIST_SAMPLES)
            self._hists[name] = bucket
        bucket.append(float(value))
        sink = self._metrics_sink
        if sink is not None:
            with suppress(Exception):
                sink.observe(name, float(value))

    def _compute_summary(self) -> dict[str, float]:
        if not self._metrics_enabled:
            return {}

        def _pctl(values: list[float], p: float) -> float:
            if not values:
                return 0.0
            vs = sorted(values)
            k = max(0, min(len(vs) - 1, round((p / 100.0) * (len(vs) - 1))))
            return float(vs[k])

        s: dict[str, float] = {}
        for key in ("fetch_duration_s", "parse_duration_s", "http_duration_s", "writer_flush_duration_s"):
            vals = list(self._hists.get(key, deque()))
            s[f"{key}_p95"] = _pctl(vals, 95.0)
            s[f"{key}_p99"] = _pctl(vals, 99.0)
        # Per-table writer latencies and rows per flush
        for key, vals_deque in self._hists.items():
            if key.startswith("writer_flush_duration_s."):
                table = key.split(".", 1)[1]
                vals = list(vals_deque)
                s[f"writer_flush_duration_s.{table}_p50"] = _pctl(vals, 50.0)
                s[f"writer_flush_duration_s.{table}_p95"] = _pctl(vals, 95.0)
                s[f"writer_flush_duration_s.{table}_p99"] = _pctl(vals, 99.0)
            if key.startswith("writer_rows."):
                table = key.split(".", 1)[1]
                vals = list(vals_deque)
                s[f"writer_rows.{table}_p50"] = _pctl(vals, 50.0)
                s[f"writer_rows.{table}_p95"] = _pctl(vals, 95.0)
                s[f"writer_rows.{table}_p99"] = _pctl(vals, 99.0)
        # DLQ aggregate counters (copy from counters for summary visibility)
        for agg_key in ("dlq_spooled_files_total", "dlq_rescued_total", "dlq_remaining_total"):
            if agg_key in self._counters:
                with suppress(Exception):
                    s[agg_key] = float(self._counters.get(agg_key, 0))
        s["rows_written_total"] = float(self._counters.get("rows_written_total", 0))
        return s

    @contextmanager
    def _span(self, name: str, **attributes: object) -> Iterator[None]:
        if not self._otel_enabled or self._tracer is None:
            yield
            return
        start_span = None
        try:
            start_span = getattr(self._tracer, "start_span", None)
        except Exception:
            start_span = None
        if callable(start_span):
            cm = None
            try:
                cm = start_span(f"vertex_forager.{name}", attributes=attributes)
            except Exception:
                cm = None
            if cm is None:
                yield
            else:
                with cm:
                    yield
        else:
            yield

    def _log_structured(
        self,
        *,
        provider: str,
        dataset: str,
        symbol: str | None,
        stage: str,
        attempt: int | None = None,
        duration_s: float | None = None,
    ) -> None:
        if not self._structured_logs:
            return
        att = attempt if attempt is not None else 0
        dur = f"{duration_s:.3f}s" if duration_s is not None else "-"
        msg = (
            f"OBS provider={sanitize_field(provider)} "
            f"dataset={sanitize_field(dataset)} "
            f"symbol={sanitize_field(symbol)} "
            f"stage={sanitize_field(stage)} "
            f"attempt={att} duration={dur}"
        )
        if self._log_verbose:
            logger.info(msg)
        else:
            logger.debug(msg)

    def _update_checkpoint(
        self,
        run_id: str,
        provider: str,
        dataset: str,
        completed_symbols: set[str],
        failed_symbols: set[str],
        pending_symbol_jobs: dict[str, list[FetchJob]],
        status: Literal["in_progress", "completed"] = "in_progress",
    ) -> None:
        """Update checkpoint with completed and failed symbols."""
        if not completed_symbols and not failed_symbols and not pending_symbol_jobs:
            return

        pending_jobs = [job for jobs in pending_symbol_jobs.values() for job in jobs]
        checkpoint = Checkpoint(
            run_id=run_id,
            provider=provider,
            dataset=dataset,
            completed=list(completed_symbols),
            failed=list(failed_symbols),
            pending_jobs=pending_jobs,
            status=status,
        )

        try:
            save_checkpoint(checkpoint)
            logger.debug("PIPELINE: Checkpoint updated for run %s", run_id)
        except Exception as e:
            logger.warning("PIPELINE: Failed to update checkpoint: %s", e)

    def _find_latest_checkpoint(self, provider: str, dataset: str) -> Checkpoint | None:
        """Find the latest checkpoint for a given provider and dataset.

        Args:
            provider: The data provider name
            dataset: The dataset name

        Returns:
            The latest checkpoint if found, None otherwise
        """
        return find_latest_checkpoint(provider, dataset)

    async def run(
        self,
        *,
        dataset: str,
        symbols: Symbols | None,
        on_progress: Callable[..., Any] | None = None,
        resume: bool = False,
        **kwargs: object,
    ) -> RunResult:
        """Execute the pipeline.

        This method sets up the asyncio queues and worker tasks, runs the pipeline
        until completion, and returns the execution result.

        Process:
        1.  Create `req_q` (for jobs) and `pkt_q` (for data packets).
        2.  Spawn `_writer_worker` tasks (consumers of processed data).
        3.  Spawn `_fetch_worker` tasks (consumers of jobs, producers of data).
        4.  Spawn `_producer` task (generator of jobs).
        5.  Wait for all tasks to complete in order:
            - Producer finishes -> Queue join.
            - Fetchers finish -> Queue join.
            - Writer workers finish.
        6.  Aggregate results and handle cleanup.

        Args:
            dataset: Name of the dataset to fetch (e.g., "sep").
            symbols: List of symbols to fetch, or None for all.
            on_progress: Optional callback to update progress bar (called on job completion).
            resume: Whether to resume from existing checkpoint (default: False).
            **kwargs: Additional arguments passed to the router's generate_jobs method.

        Returns:
            RunResult: Summary of the run including metrics and errors.

        Raises:
            RuntimeError: Orchestration-level failures (e.g., early writer shutdown)
                or if ``run()`` is called while another run is already in progress.

        Notes:
            - Exceptions raised during fetch/parse/write (e.g., httpx.RequestError,
              httpx.HTTPStatusError, ValidationError, PrimaryKeyMissingError,
              PrimaryKeyNullError) are captured and appended to `RunResult.errors`
              and are not re-raised by default.
            - Callers should inspect `RunResult.errors` for per-task failures and
              only expect orchestration-level issues to raise.
            - When `resume=True`, symbols already completed in previous runs will be skipped
              based on checkpoint rows stored in ~/.cache/vertex-forager/state.db.
        """
        if self._running:
            raise RuntimeError("Pipeline is already running; concurrent run() calls are not supported")
        self._running = True

        try:
            run_id, completed_symbols = self._initialize_run_state(dataset=dataset, resume=resume)
            req_q, pkt_q = self._create_run_queues()
            self._init_metrics_for_run()
            t_run0 = time.monotonic()
            result, result_lock = self._create_run_result(run_id=run_id, dataset=dataset)
            concurrency = self._resolve_concurrency()
            self._ensure_parse_executor(concurrency)
            producer_task, fetch_tasks, writer_tasks = self._create_pipeline_tasks(
                req_q=req_q,
                pkt_q=pkt_q,
                result=result,
                result_lock=result_lock,
                dataset=dataset,
                symbols=symbols,
                completed_symbols=completed_symbols,
                concurrency=concurrency,
                on_progress=on_progress,
                kwargs=kwargs,
            )
            self._register_run_runtime(
                producer_task=producer_task,
                fetch_tasks=fetch_tasks,
                writer_tasks=writer_tasks,
                req_q=req_q,
                pkt_q=pkt_q,
            )
            await self._await_pipeline_completion(
                dataset=dataset,
                req_q=req_q,
                pkt_q=pkt_q,
                producer_task=producer_task,
                fetch_tasks=fetch_tasks,
                writer_tasks=writer_tasks,
                concurrency=concurrency,
            )

            await self._finalize_run(
                result=result,
                dataset=dataset,
                run_id=run_id,
                started_monotonic=t_run0,
            )
            return result
        finally:
            try:
                await self.stop()
            finally:
                self._running = False

    def _create_pipeline_tasks(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
        dataset: str,
        symbols: Sequence[str] | None,
        completed_symbols: set[str],
        concurrency: int,
        on_progress: Callable[..., Any] | None,
        kwargs: dict[str, object],
    ) -> tuple[asyncio.Task[Any], list[asyncio.Task[Any]], list[asyncio.Task[Any]]]:
        writer_tasks = [
            asyncio.create_task(
                self._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock),
                name=f"vertex-forager:writer:{i}",
            )
            for i in range(self._config.writer_concurrency)
        ]
        order_counter = itertools.count()
        fetch_tasks = [
            asyncio.create_task(
                self._fetch_worker(
                    i,
                    req_q=req_q,
                    pkt_q=pkt_q,
                    result=result,
                    result_lock=result_lock,
                    order_counter=order_counter,
                    on_progress=on_progress,
                ),
                name=f"vertex-forager:fetch:{i}",
            )
            for i in range(concurrency)
        ]
        producer_task = asyncio.create_task(
            self._producer(
                req_q=req_q,
                dataset=dataset,
                symbols=symbols,
                order_counter=order_counter,
                completed_symbols=completed_symbols,
                pending_symbol_jobs=self._pending_symbol_jobs,
                **kwargs,
            ),
            name="vertex-forager:producer",
        )
        return producer_task, fetch_tasks, writer_tasks

    def _register_run_runtime(
        self,
        *,
        producer_task: asyncio.Task[Any],
        fetch_tasks: list[asyncio.Task[Any]],
        writer_tasks: list[asyncio.Task[Any]],
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
    ) -> None:
        self._active_tasks = [producer_task, *fetch_tasks, *writer_tasks]
        self._req_q = req_q
        self._pkt_q = pkt_q
        self._writer_tasks = writer_tasks
        self._fair_state = FairnessState()
        self._writer_flush_attempted = False
        self._writer_flushed = False
        self._flush_lock = asyncio.Lock()

    async def _orchestrate_pipeline(
        self,
        *,
        dataset: str,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        producer_task: asyncio.Task[Any],
        fetch_tasks: list[asyncio.Task[Any]],
        writer_tasks: list[asyncio.Task[Any]],
        concurrency: int,
    ) -> None:
        with self._span("pipeline", provider=self._router.provider, dataset=dataset):
            logger.info("PIPELINE: Starting producer task...")
            self._log_structured(
                provider=self._router.provider,
                dataset=dataset,
                symbol="*",
                stage="producer_start",
            )
            await producer_task
            logger.info("PIPELINE: Producer completed, waiting for request queue...")
            self._log_structured(
                provider=self._router.provider,
                dataset=dataset,
                symbol="*",
                stage="producer_done",
            )
            if self._metrics_enabled:
                with suppress(Exception):
                    self._summary["req_q_len_after_producer"] = float(req_q.qsize())
                    self._summary["pkt_q_len_after_producer"] = float(pkt_q.qsize())
        await req_q.join()
        logger.info("PIPELINE: Request queue joined, sending sentinel signals...")
        if self._metrics_enabled:
            with suppress(Exception):
                self._summary["req_q_len_after_req_join"] = float(req_q.qsize())
        for _ in range(concurrency):
            await req_q.put((self.PRIORITY_SENTINEL, 0, None))
        logger.info("PIPELINE: Waiting for fetch tasks to complete...")
        await asyncio.gather(*fetch_tasks)
        logger.info("PIPELINE: Fetch tasks completed, waiting for packet queue...")
        await pkt_q.join()
        if self._metrics_enabled:
            with suppress(Exception):
                self._summary["pkt_q_len_after_pkt_join"] = float(pkt_q.qsize())
        for _ in writer_tasks:
            await pkt_q.put(None)

    async def _await_pipeline_completion(
        self,
        *,
        dataset: str,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        producer_task: asyncio.Task[Any],
        fetch_tasks: list[asyncio.Task[Any]],
        writer_tasks: list[asyncio.Task[Any]],
        concurrency: int,
    ) -> None:
        writer_monitor = cast("asyncio.Future[Any]", asyncio.gather(*writer_tasks))
        fetch_monitor = cast("asyncio.Future[Any]", asyncio.gather(*fetch_tasks))
        orchestrator = asyncio.create_task(
            self._orchestrate_pipeline(
                dataset=dataset,
                req_q=req_q,
                pkt_q=pkt_q,
                producer_task=producer_task,
                fetch_tasks=fetch_tasks,
                writer_tasks=writer_tasks,
                concurrency=concurrency,
            ),
            name="vertex-forager:orchestrator",
        )
        self._active_tasks.append(orchestrator)
        futures: set[asyncio.Future[Any]] = {writer_monitor, fetch_monitor, cast("asyncio.Future[Any]", orchestrator)}
        done, _pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
        self._raise_if_early_failure(
            done=done,
            orchestrator=orchestrator,
            fetch_monitor=fetch_monitor,
            writer_monitor=writer_monitor,
        )
        await orchestrator
        logger.debug("PIPELINE: Waiting for writer tasks to complete...")
        await writer_monitor
        await fetch_monitor

    def _initialize_run_state(self, *, dataset: str, resume: bool) -> tuple[str, set[str]]:
        run_id, completed_symbols, failed_symbols = initialize_run_state_impl(
            provider=self._router.provider,
            dataset=dataset,
            resume=resume,
            find_latest_checkpoint=self._find_latest_checkpoint,
            logger=logger,
        )
        self._run_id = run_id
        self._completed_symbols = set(completed_symbols)
        self._failed_symbols = set(failed_symbols)
        self._pending_symbol_jobs = {}
        if resume:
            latest_checkpoint = self._find_latest_checkpoint(self._router.provider, dataset)
            if latest_checkpoint is not None:
                for pending_job in latest_checkpoint.pending_jobs:
                    if pending_job.symbol:
                        self._pending_symbol_jobs.setdefault(pending_job.symbol, []).append(pending_job)
        return run_id, completed_symbols

    def _create_run_queues(
        self,
    ) -> tuple[
        asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        asyncio.Queue[FramePacket | None],
    ]:
        req_q, pkt_q = create_run_queues_impl(
            queue_max=self._config.queue_max,
            checkpoint_retention_days=self._config.checkpoint_retention_days,
            run_history_retention_days=self._config.run_history_retention_days,
            dlq_tmp_periodic_cleanup=self._config.dlq_tmp_periodic_cleanup,
            dlq_tmp_retention_s=self._config.dlq_tmp_retention_s,
            cache_dir=get_cache_dir(),
            logger=logger,
        )
        return (
            cast("asyncio.PriorityQueue[tuple[int, int, FetchJob | None]]", req_q),
            cast("asyncio.Queue[FramePacket | None]", pkt_q),
        )

    def _init_metrics_for_run(self) -> None:
        counters, hists, summary = init_metrics_for_run_impl(metrics_enabled=self._metrics_enabled)
        if not self._metrics_enabled:
            return
        self._counters = counters
        self._hists = hists
        self._summary = summary

    def _create_run_result(self, *, run_id: str, dataset: str) -> tuple[RunResult, asyncio.Lock]:
        result, result_lock = create_run_result_impl(
            provider=self._router.provider,
            run_id=run_id,
            dataset=dataset,
        )
        self._run_result = result
        self._run_result_lock = result_lock
        return result, result_lock

    def _resolve_concurrency(self) -> int:
        try:
            return max(1, int(self.controller.concurrency_limit))
        except (TypeError, ValueError):
            return 1

    def _ensure_parse_executor(self, concurrency: int) -> None:
        if getattr(self, "_parse_executor", None) is None or getattr(self._parse_executor, "_shutdown", False):
            self._parse_executor = ThreadPoolExecutor(
                max_workers=concurrency,
                thread_name_prefix="vertex-forager:parse",
            )

    def _raise_if_early_failure(
        self,
        *,
        done: set[asyncio.Future[Any]],
        orchestrator: asyncio.Task[Any],
        fetch_monitor: asyncio.Future[Any],
        writer_monitor: asyncio.Future[Any],
    ) -> None:
        if fetch_monitor in done and orchestrator not in done:
            exc = fetch_monitor.exception()
            if exc:
                raise exc
        if writer_monitor in done and orchestrator not in done:
            exc = writer_monitor.exception()
            if exc:
                raise exc
            raise RuntimeError("Writer exited prematurely")

    async def _finalize_run(
        self,
        *,
        result: RunResult,
        dataset: str,
        run_id: str,
        started_monotonic: float,
    ) -> None:
        logger.debug("PIPELINE: Flushing writer buffer...")
        await self._try_flush_once(suppress=False, consume=True)
        logger.info("PIPELINE: Run completed. Total errors: %s", len(result.errors))
        if self._metrics_enabled:
            self._merge_component_counters()
            result.metrics_counters = dict(self._counters)
            result.metrics_histograms = {k: list(v) for k, v in self._hists.items()}
            self._summary = self._compute_summary()
            result.metrics_summary = dict(self._summary)
            sink = self._metrics_sink
            if sink is not None:
                with suppress(Exception):
                    sink.summary(dict(self._summary))
            self._emit_pipeline_summary_log(dataset=dataset, started_monotonic=started_monotonic)
        result.finished_at = time.time()
        result.duration_s = time.monotonic() - started_monotonic
        async with self._checkpoint_lock:
            self._update_checkpoint(
                run_id,
                self._router.provider,
                dataset,
                self._completed_symbols,
                self._failed_symbols,
                self._pending_symbol_jobs,
                status="completed" if not self._failed_symbols and not self._pending_symbol_jobs else "in_progress",
            )
        try:
            save_run_history(result, run_id)
            logger.debug("PIPELINE: Run history saved for run %s", run_id)
        except Exception as e:
            logger.warning("PIPELINE: Failed to save run history: %s", e)

    def _merge_component_counters(self) -> None:
        self._run_finalizer.merge_component_counters()

    def _emit_pipeline_summary_log(self, *, dataset: str, started_monotonic: float) -> None:
        self._run_finalizer.emit_pipeline_summary_log(
            dataset=dataset,
            started_monotonic=started_monotonic,
            summary=self._summary,
        )

    async def stop(self) -> None:
        task = getattr(self, "_stop_task", None)
        if task is not None:
            if not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    # Ensure internal cleanup finishes before propagating cancel
                    try:
                        await asyncio.shield(task)
                    finally:
                        self._stop_task = None
                    raise
                else:
                    return
            self._stop_task = None
        self._stop_task = asyncio.create_task(self._stop_impl())
        try:
            try:
                await asyncio.shield(self._stop_task)
            except asyncio.CancelledError:
                # Ensure internal cleanup completes before re-raising
                try:
                    await asyncio.shield(self._stop_task)
                finally:
                    self._stop_task = None
                raise
        finally:
            self._stop_task = None

    async def _stop_impl(self) -> None:
        """Gracefully stop the pipeline.

        Cancels all running tasks (producer, fetchers, writers) and awaits their
        cleanup. This method is idempotent and safe to call multiple times.

        Notes:
            - Internally, this method calls `asyncio.gather(*self._active_tasks, return_exceptions=True)`,
              which collects `asyncio.CancelledError` as a returned exception rather than raising it.
            - `asyncio.CancelledError` would only propagate if the `stop` coroutine itself is externally
              cancelled by the caller while awaiting completion.

        Raises:
            asyncio.CancelledError: Only if this `stop` coroutine is externally cancelled;
                exceptions from tasks are logged and suppressed via ``return_exceptions=True``.
        """
        if not self._active_tasks:
            return

        logger.debug("PIPELINE: Stopping pipeline...")
        writer_tasks = cast("list[asyncio.Task[Any]] | None", getattr(self, "_writer_tasks", None))
        writer_set: set[asyncio.Task[Any]] = set(writer_tasks) if writer_tasks else set()
        await self._cancel_non_writer_tasks(writer_set)
        await self._enqueue_request_sentinels()
        active_writers, sentinel_put_tasks = self._schedule_packet_sentinels(writer_tasks)
        timed_out = await self._await_packet_sentinel_tasks(
            writer_set=writer_set,
            sentinel_put_tasks=sentinel_put_tasks,
        )
        if timed_out:
            writer_set = set()
            await self._drain_pending_packets()
        elif active_writers == 0:
            await self._drain_pending_packets()
        await self._await_writer_tasks(writer_set)
        self._active_tasks.clear()
        try:
            await self._drain_pending_packets()
        except Exception:
            logger.debug("PIPELINE: Safety-net pkt_q drain raised but suppressed", exc_info=True)
        try:
            await self._try_flush_once(suppress=True, consume=False)
        except Exception:
            logger.debug("PIPELINE: Writer flush on stop raised but suppressed", exc_info=True)
        try:
            await asyncio.to_thread(self._parse_executor.shutdown, wait=True)
        except (RuntimeError, ValueError) as e:
            logger.exception("PIPELINE: Parse executor shutdown failed: %s", e)
        except Exception:
            logger.exception("PIPELINE: Unexpected error during parse executor shutdown")
        logger.debug("PIPELINE: Pipeline stopped.")

    async def _cancel_non_writer_tasks(self, writer_set: set[asyncio.Task[Any]]) -> None:
        await cancel_non_writer_tasks_impl(
            active_tasks=cast("list[asyncio.Task[Any]]", self._active_tasks),
            writer_set=writer_set,
        )

    async def _enqueue_request_sentinels(self) -> None:
        req_q = cast("asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] | None", getattr(self, "_req_q", None))
        fetch_n = int(getattr(self.controller, "concurrency_limit", 0) or 0)
        await enqueue_request_sentinels_impl(
            req_q=cast("asyncio.PriorityQueue[tuple[int, int, object | None]] | None", req_q),
            fetch_n=fetch_n,
            priority_sentinel=self.PRIORITY_SENTINEL,
            logger=logger,
        )

    def _schedule_packet_sentinels(
        self, writer_tasks: list[asyncio.Task[Any]] | None
    ) -> tuple[int, list[asyncio.Task[Any]]]:
        pkt_q = cast("asyncio.Queue[FramePacket | None] | None", getattr(self, "_pkt_q", None))
        return schedule_packet_sentinels_impl(
            pkt_q=cast("asyncio.Queue[object | None] | None", pkt_q),
            writer_tasks=writer_tasks,
            logger=logger,
        )

    async def _await_packet_sentinel_tasks(
        self,
        *,
        writer_set: set[asyncio.Task[Any]],
        sentinel_put_tasks: list[asyncio.Task[Any]],
    ) -> bool:
        return await await_packet_sentinel_tasks_impl(
            writer_set=writer_set,
            sentinel_put_tasks=sentinel_put_tasks,
            logger=logger,
        )

    async def _drain_pending_packets(self) -> None:
        pkt_q = getattr(self, "_pkt_q", None)
        if pkt_q is None:
            return
        drained: list[FramePacket] = []
        while True:
            try:
                pkt = pkt_q.get_nowait()
            except asyncio.QueueEmpty:
                break
            if pkt is None:
                with suppress(Exception):
                    pkt_q.task_done()
                continue
            drained.append(pkt)
        if not drained:
            return
        res = getattr(self, "_run_result", None)
        res_lock = getattr(self, "_run_result_lock", None)
        await self._persist_packets_with_dlq(drained, res, res_lock)
        for _ in drained:
            with suppress(Exception):
                pkt_q.task_done()

    async def _await_writer_tasks(self, writer_set: set[asyncio.Task[Any]]) -> None:
        await await_writer_tasks_impl(
            writer_set=writer_set,
            logger=logger,
        )

    async def _producer(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        dataset: str,
        symbols: Symbols | None,
        order_counter: itertools.count,
        completed_symbols: set[str] | None = None,
        pending_symbol_jobs: dict[str, list[FetchJob]] | None = None,
        **kwargs: object,
    ) -> None:
        """Generate fetch jobs and push them to the request queue.

        Iterates through the provider's `generate_jobs` generator. Once all jobs are
        enqueued, fetch workers may enqueue additional jobs (e.g., pagination).
        """
        job_count = 0
        logger.debug(
            "PRODUCER: Starting job generation for dataset=%s, symbols=%s",
            dataset,
            len(symbols) if symbols else "all",
        )

        pending_symbols = set(pending_symbol_jobs or {})
        for pending_jobs in (pending_symbol_jobs or {}).values():
            for pending_job in pending_jobs:
                await req_q.put((self.PRIORITY_PAGINATION, next(order_counter), pending_job))
                job_count += 1
                self._inc("jobs_generated", 1)

        async for job in self._router.generate_jobs(dataset=dataset, symbols=symbols, **kwargs):
            # Skip already completed symbols if resume is enabled
            if completed_symbols and job.symbol and job.symbol in completed_symbols:
                logger.debug("PRODUCER: Skipping already completed symbol %s", job.symbol)
                continue
            if job.symbol and job.symbol in pending_symbols:
                logger.debug("PRODUCER: Resuming paginated symbol %s from pending checkpoint job", job.symbol)
                continue

            await req_q.put((self.PRIORITY_NEW_JOB, next(order_counter), job))
            job_count += 1
            self._inc("jobs_generated", 1)
            if job_count % 100 == 0:
                logger.debug("PRODUCER: Generated %s jobs so far...", job_count)

        logger.debug("PRODUCER: Completed job generation. Total jobs: %s", job_count)

    async def _try_flush_once(self, *, suppress: bool, consume: bool = True) -> None:
        flush_lock = getattr(self, "_flush_lock", None)
        if flush_lock is None:
            flush_lock = asyncio.Lock()
            self._flush_lock = flush_lock

        async with flush_lock:
            if not consume:
                if getattr(self, "_writer_flushed", False):
                    return
                try:
                    await self._writer.flush()
                    self._writer_flushed = True
                except Exception:
                    if suppress:
                        logger.debug("PIPELINE: Writer flush attempt suppressed", exc_info=True)
                        return
                    raise
                return
            if not getattr(self, "_writer_flush_attempted", False) and not getattr(self, "_writer_flushed", False):
                self._writer_flush_attempted = True
                try:
                    await self._writer.flush()
                    self._writer_flushed = True
                except Exception:
                    if suppress:
                        logger.debug("PIPELINE: Writer flush attempt suppressed", exc_info=True)
                    else:
                        raise

    async def _persist_packets_with_dlq(
        self,
        packets: list[FramePacket],
        result: RunResult | None,
        result_lock: asyncio.Lock | None,
    ) -> None:
        for pkt in packets:
            try:
                wr = await self._writer.write(pkt)
                if result is not None and result_lock is not None:
                    async with result_lock:
                        result.tables[wr.table] = result.tables.get(wr.table, 0) + wr.rows
                self._inc("rows_written_total", int(wr.rows))
            except Exception as e:
                dlq_enabled = bool(getattr(self._config, "dlq_enabled", True))
                if dlq_enabled:
                    try:
                        dlq_dir = get_cache_dir() / "dlq" / pkt.table
                        dlq_dir.mkdir(parents=True, exist_ok=True)
                        ts_ns = time.time_ns()
                        fpath = dlq_dir / f"packet_{ts_ns}.ipc"
                        tmp_path = fpath.parent / (f"{fpath.name}.tmp")
                        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                        with os.fdopen(fd, "wb") as fh:
                            pkt.frame.write_ipc(fh)
                            fh.flush()
                            os.fsync(fh.fileno())
                        os.replace(tmp_path, fpath)
                        with suppress(Exception):
                            dir_fd = os.open(str(dlq_dir), os.O_RDONLY)
                            try:
                                os.fsync(dir_fd)
                            finally:
                                os.close(dir_fd)
                        self._inc("dlq_spooled_files_total", 1)
                        self._inc(f"dlq_spooled_files.{pkt.table}", 1)
                    except Exception as e2:
                        logger.exception("PIPELINE: DLQ spool failed for drained packet: %s", e2)
                        if result is not None and result_lock is not None:
                            async with result_lock:
                                pending = result.dlq_pending.get(pkt.table, [])
                                pending.append(pkt)
                                result.dlq_pending[pkt.table] = pending
                if result is not None and result_lock is not None:
                    async with result_lock:
                        entry = result.dlq_counts.get(pkt.table) or {"rescued": 0, "remaining": 0}
                        entry["remaining"] = entry.get("remaining", 0) + 1
                        result.dlq_counts[pkt.table] = entry
                        result.errors.append(RunError.from_exception(e, pkt.provider, pkt.table, ""))

    async def _fetch_worker(
        self,
        worker_id: int,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
        order_counter: itertools.count,
        on_progress: Callable[..., Any] | None = None,
    ) -> None:
        """Consume jobs, execute HTTP requests, and produce data packets.

        Cycle:
        1.  Get `FetchJob` from `req_q`.
        2.  Execute HTTP request via `_fetch_with_retry` (handles rate limits/retries).
        3.  Parse response using `provider.parse`.
        4.  Map schema using `mapper`.
        5.  Put resulting `FramePacket`s into `pkt_q`.
        6.  Log errors to `result` if exceptions occur.
        """
        handler = self._build_progress_handler(on_progress)
        job_count = 0
        burst_cap = getattr(self._config, "pagination_max_burst", None)
        deferred_demotes: deque[tuple[int, int, FetchJob]] = deque()
        while True:
            self._drain_deferred_demotes(req_q=req_q, deferred_demotes=deferred_demotes)
            selected = await self._dequeue_worker_job(
                req_q=req_q,
                burst_cap=burst_cap,
            )
            priority = selected.priority
            job = selected.job
            demote_jobs = selected.demoted
            already_done = selected.already_done
            self._requeue_demoted_jobs(
                req_q=req_q,
                demote_jobs=demote_jobs,
                deferred_demotes=deferred_demotes,
                order_counter=order_counter,
            )
            if self._should_exit_worker_for_sentinel(
                job=job,
                priority=priority,
                already_done=already_done,
                req_q=req_q,
                deferred_demotes=deferred_demotes,
                worker_id=worker_id,
                job_count=job_count,
            ):
                return
            if job is None:
                await asyncio.sleep(0)
                continue
            job_count += 1
            self._inc("jobs_processed", 1)
            if job_count % 100 == 0:
                logger.debug("[Worker-%s] Processed %s jobs so far...", worker_id, job_count)
            payload: bytes | None = None
            worker_exc: BaseException | None = None
            parse_result: ParseResult | None = None
            try:
                payload, worker_exc, parse_result = await self._process_worker_job(
                    worker_id=worker_id,
                    priority=priority,
                    job=job,
                    req_q=req_q,
                    pkt_q=pkt_q,
                    result=result,
                    result_lock=result_lock,
                    order_counter=order_counter,
                )
            except asyncio.CancelledError as exc:
                worker_exc = exc
                raise
            finally:
                req_q.task_done()
                try:
                    await handler(job, payload, worker_exc, parse_result)
                except Exception as e:
                    logger.error(
                        "[Worker-%s] Error in result handler for %s:%s:%s: %s",
                        worker_id,
                        job.provider,
                        job.dataset,
                        job.symbol,
                        e,
                    )
                try:
                    await self._record_worker_symbol_state(job=job, worker_exc=worker_exc, parse_result=parse_result)
                except Exception as e:
                    logger.error(
                        "[Worker-%s] Error recording symbol state for %s:%s:%s: %s",
                        worker_id,
                        job.provider,
                        job.dataset,
                        job.symbol,
                        e,
                    )

    def _build_progress_handler(
        self,
        on_progress: Callable[..., Any] | None,
    ) -> Callable[[FetchJob, bytes | None, BaseException | None, ParseResult | None], Awaitable[None]]:
        async def noop_handler(
            job: FetchJob,
            payload: bytes | None,
            exc: BaseException | None,
            parse_result: ParseResult | None,
        ) -> None:
            return

        if on_progress is None:
            return noop_handler
        try:
            sig = inspect.signature(on_progress)
            wants_parse_result = "parse_result" in sig.parameters
            is_async = inspect.iscoroutinefunction(on_progress)
        except Exception as e:
            logger.warning("Failed to bind on_progress handler: %s", e)
            return noop_handler

        async def _progress_wrapper(
            job: FetchJob,
            payload: bytes | None,
            exc: BaseException | None,
            parse_result: ParseResult | None,
        ) -> None:
            kwargs: dict[str, object] = {"job": job, "payload": payload, "exc": exc}
            if wants_parse_result:
                kwargs["parse_result"] = parse_result
            try:
                if is_async:
                    await on_progress(**kwargs)
                else:
                    on_progress(**kwargs)
            except Exception as e:
                logger.error("Error in on_progress callback: %s", e)

        return _progress_wrapper

    def _drain_deferred_demotes(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        deferred_demotes: deque[tuple[int, int, FetchJob]],
    ) -> None:
        while deferred_demotes:
            try:
                req_q.put_nowait(deferred_demotes[0])
                deferred_demotes.popleft()
                req_q.task_done()
            except asyncio.QueueFull:
                break

    async def _dequeue_worker_job(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        burst_cap: int | None,
    ) -> SchedulerResult:
        if burst_cap is None:
            priority, _, job = await req_q.get()
            already_done = False
            if job is None:
                req_q.task_done()
                already_done = True
            return SchedulerResult(priority=priority, job=job, demoted=[], already_done=already_done)
        if not isinstance(burst_cap, int):
            raise RuntimeError("pagination_max_burst must be int when fairness dequeue is enabled")
        return await pop_next_job_respecting_fairness_impl(
            req_q=req_q,
            fair_lock=self._fair_lock,
            burst_cap=burst_cap,
            priority_pagination=self.PRIORITY_PAGINATION,
            priority_new_job=self.PRIORITY_NEW_JOB,
            fairness_state=self._fair_state,
        )

    def _requeue_demoted_jobs(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        demote_jobs: list[FetchJob],
        deferred_demotes: deque[tuple[int, int, FetchJob]],
        order_counter: itertools.count,
    ) -> None:
        for demoted_job in demote_jobs:
            requeued_item = (self.PRIORITY_NEW_JOB, next(order_counter), demoted_job)
            try:
                req_q.put_nowait(requeued_item)
                req_q.task_done()
            except asyncio.QueueFull:
                deferred_demotes.append(requeued_item)

    def _should_exit_worker_for_sentinel(
        self,
        *,
        job: FetchJob | None,
        priority: int,
        already_done: bool,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        deferred_demotes: deque[tuple[int, int, FetchJob]],
        worker_id: int,
        job_count: int,
    ) -> bool:
        if job is not None:
            return False
        if not (already_done and priority == self.PRIORITY_SENTINEL):
            return False
        for _ in deferred_demotes:
            req_q.task_done()
        if deferred_demotes:
            logger.warning(
                "[Worker-%s] Dropping %s deferred demoted jobs on shutdown",
                worker_id,
                len(deferred_demotes),
            )
        logger.debug(
            "[Worker-%s] Received sentinel, shutting down. Total jobs processed: %s",
            worker_id,
            job_count,
        )
        return True

    async def _process_worker_job(
        self,
        *,
        worker_id: int,
        priority: int,
        job: FetchJob,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
        order_counter: itertools.count,
    ) -> tuple[bytes | None, BaseException | None, ParseResult | None]:
        payload: bytes | None = None
        worker_exc: BaseException | None = None
        parse_result: ParseResult | None = None
        try:
            payload = await self._fetch_payload(worker_id=worker_id, priority=priority, job=job)
            parse_result = await self._parse_payload(job=job, payload=payload, worker_id=worker_id)
            await self._emit_packets_and_next_jobs(
                parse_result=parse_result,
                req_q=req_q,
                pkt_q=pkt_q,
                order_counter=order_counter,
                worker_id=worker_id,
                job=job,
            )
        except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as exc:
            worker_exc = exc
            await self._record_worker_error(
                result=result,
                result_lock=result_lock,
                job=job,
                exc=exc,
                worker_id=worker_id,
                stage="error",
            )
        except FetchError as exc:
            worker_exc = exc
            await self._record_worker_error(
                result=result,
                result_lock=result_lock,
                job=job,
                exc=exc,
                worker_id=worker_id,
                stage="error_fetch",
            )
        except Exception as exc:
            worker_exc = exc
            await self._record_worker_error(
                result=result,
                result_lock=result_lock,
                job=job,
                exc=exc,
                worker_id=worker_id,
                stage="error_unexpected",
            )
        return payload, worker_exc, parse_result

    async def _fetch_payload(self, *, worker_id: int, priority: int, job: FetchJob) -> bytes:
        return await fetch_payload_impl(
            worker_id=worker_id,
            priority=priority,
            job=job,
            fetch_with_retry=self._fetch_with_retry,
            span=self._span,
            observe=self._observe,
            log_structured=self._log_structured,
            logger=logger,
        )

    async def _parse_payload(self, *, job: FetchJob, payload: bytes, worker_id: int) -> ParseResult:
        return await parse_payload_impl(
            job=job,
            payload=payload,
            worker_id=worker_id,
            parse_executor=self._parse_executor,
            router_parse=self._router.parse,
            span=self._span,
            observe=self._observe,
            log_structured=self._log_structured,
            logger=logger,
        )

    async def _emit_packets_and_next_jobs(
        self,
        *,
        parse_result: ParseResult,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        order_counter: itertools.count,
        worker_id: int,
        job: FetchJob,
    ) -> None:
        await emit_packets_and_next_jobs_impl(
            parse_result=parse_result,
            req_q=req_q,
            pkt_q=pkt_q,
            order_counter=order_counter,
            worker_id=worker_id,
            job=job,
            parse_executor=self._parse_executor,
            normalize_packet=self._mapper.normalize if self._mapper is not None else (lambda *, packet: packet),
            inc=self._inc,
            priority_pagination=self.PRIORITY_PAGINATION,
            logger=logger,
        )

    async def _record_worker_error(
        self,
        *,
        result: RunResult,
        result_lock: asyncio.Lock,
        job: FetchJob,
        exc: Exception,
        worker_id: int,
        stage: str,
    ) -> None:
        await record_worker_error_impl(
            result=result,
            result_lock=result_lock,
            job=job,
            exc=exc,
            worker_id=worker_id,
            stage=stage,
            inc=self._inc,
            log_structured=self._log_structured,
            logger=logger,
        )

    async def _record_worker_symbol_state(
        self, *, job: FetchJob, worker_exc: BaseException | None, parse_result: Any = None
    ) -> None:
        if not job.symbol:
            return
        async with self._checkpoint_lock:
            if worker_exc is None:
                has_more_pages = parse_result is not None and bool(parse_result.next_jobs)
                if has_more_pages:
                    self._completed_symbols.discard(job.symbol)
                    self._failed_symbols.discard(job.symbol)
                    self._pending_symbol_jobs[job.symbol] = [
                        next_job for next_job in parse_result.next_jobs if next_job.symbol == job.symbol
                    ]
                else:
                    self._failed_symbols.discard(job.symbol)
                    self._pending_symbol_jobs.pop(job.symbol, None)
                    self._completed_symbols.add(job.symbol)
            else:
                self._completed_symbols.discard(job.symbol)
                self._failed_symbols.add(job.symbol)
            if self._run_id and job.dataset:
                self._update_checkpoint(
                    self._run_id,
                    self._router.provider,
                    job.dataset,
                    self._completed_symbols,
                    self._failed_symbols,
                    self._pending_symbol_jobs,
                )

    async def _validate_data_quality(
        self,
        *,
        table: str,
        df: pl.DataFrame,
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        await validate_data_quality_impl(
            table=table,
            df=df,
            result=result,
            result_lock=result_lock,
            logger=logger,
        )

    async def _writer_worker(
        self,
        *,
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        ctx = self._writer_context
        await writer_worker(
            pkt_q=pkt_q,
            result=result,
            result_lock=result_lock,
            flush_threshold=self._flush_threshold,
            flush_all_writer_buffers=self._flush_all_writer_buffers,
            buffer_or_flush_packet=self._buffer_or_flush_packet,
            flush_on_writer_cancel=self._flush_on_writer_cancel,
            dlq_spool_error_cls=ctx.dlq_spool_error_cls,
            logger=ctx.logger,
        )

    async def _flush_on_writer_cancel(
        self,
        *,
        buffers: dict[str, list[FramePacket]],
        buffer_rows: dict[str, int],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        ctx = self._writer_context
        await flush_on_writer_cancel(
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
            flush_writer_table=self._flush_writer_table,
            logger=ctx.logger,
        )

    async def _flush_all_writer_buffers(
        self,
        *,
        buffers: dict[str, list[FramePacket]],
        buffer_rows: dict[str, int],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        ctx = self._writer_context
        await flush_all_writer_buffers(
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
            flush_writer_table=self._flush_writer_table,
            logger=ctx.logger,
        )

    async def _buffer_or_flush_packet(
        self,
        *,
        packet: FramePacket,
        threshold: int,
        buffers: dict[str, list[FramePacket]],
        buffer_rows: dict[str, int],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        ctx = self._writer_context
        await buffer_or_flush_packet(
            packet=packet,
            threshold=threshold,
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
            flush_writer_table=self._flush_writer_table,
            progress_log_chunk_rows=ctx.progress_log_chunk_rows,
            logger=ctx.logger,
        )

    async def _flush_writer_table(
        self,
        *,
        table: str,
        buffers: dict[str, list[FramePacket]],
        buffer_rows: dict[str, int],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        ctx = self._writer_context

        def _concat_frames_with_flex(
            *,
            frames: list[pl.DataFrame],
            table_name: str,
            schema: object,
            rechunk: bool,
        ) -> pl.DataFrame:
            return concat_frames_with_flex(
                frames=frames,
                table_name=table_name,
                schema=schema,
                rechunk=rechunk,
                router_flexible_schema=ctx.router_flexible_schema,
                logger=ctx.logger,
            )

        async def _write_merged_packet(
            *,
            table: str,
            packet: FramePacket,
            stage: str,
            result: RunResult,
            result_lock: asyncio.Lock,
        ) -> None:
            await write_merged_packet(
                table=table,
                packet=packet,
                stage=stage,
                result=result,
                result_lock=result_lock,
                writer=ctx.writer,
                span=ctx.span,
                inc=ctx.inc,
                observe=ctx.observe,
                log_structured=ctx.log_structured,
            )

        async def _handle_writer_flush_error(
            *,
            table: str,
            packets: list[FramePacket],
            exc: Exception,
            prefix: str,
            buffers: dict[str, list[FramePacket]],
            buffer_rows: dict[str, int],
            result: RunResult,
            result_lock: asyncio.Lock,
        ) -> None:
            await handle_writer_flush_error(
                table=table,
                packets=packets,
                exc=exc,
                prefix=prefix,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
                writer=ctx.writer,
                config=ctx.config,
                inc=ctx.inc,
                log_structured=ctx.log_structured,
                spool_to_dlq_and_rescue=ctx.spool_to_dlq_and_rescue,
                build_writer_error_summary=ctx.build_writer_error_summary,
                dlq_spool_error_cls=ctx.dlq_spool_error_cls,
                logger=ctx.logger,
            )

        async def _flush_chunked_table(
            *,
            table: str,
            packets: list[FramePacket],
            schema: object,
            chunk_size: int,
            buffers: dict[str, list[FramePacket]],
            buffer_rows: dict[str, int],
            result: RunResult,
            result_lock: asyncio.Lock,
        ) -> None:
            await flush_chunked_table(
                table=table,
                packets=packets,
                schema=schema,
                chunk_size=chunk_size,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
                concat_frames_with_flex=_concat_frames_with_flex,
                validate_unique_key=validate_unique_key,
                validate_data_quality=ctx.validate_data_quality,
                write_merged_packet=_write_merged_packet,
                handle_writer_flush_error=_handle_writer_flush_error,
                compute_error_cls=ctx.compute_error_cls,
                validation_error_cls=ctx.validation_error_cls,
                primary_key_missing_error_cls=PrimaryKeyMissingError,
                primary_key_null_error_cls=PrimaryKeyNullError,
                dlq_spool_error_cls=ctx.dlq_spool_error_cls,
                duckdb_module=ctx.duckdb_module,
                log_structured=ctx.log_structured,
                logger=ctx.logger,
            )

        async def _flush_legacy_table(
            *,
            table: str,
            packets: list[FramePacket],
            schema: object,
            result: RunResult,
            result_lock: asyncio.Lock,
        ) -> None:
            await flush_legacy_table(
                table=table,
                packets=packets,
                schema=schema,
                result=result,
                result_lock=result_lock,
                concat_frames_with_flex=_concat_frames_with_flex,
                validate_unique_key=validate_unique_key,
                validate_data_quality=ctx.validate_data_quality,
                write_merged_packet=_write_merged_packet,
                logger=ctx.logger,
            )

        await flush_writer_table(
            table=table,
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
            get_table_schema=ctx.get_table_schema,
            writer_chunk_rows=getattr(ctx.config, "writer_chunk_rows", None),
            flush_chunked_table=_flush_chunked_table,
            flush_legacy_table=_flush_legacy_table,
            handle_writer_flush_error=_handle_writer_flush_error,
            compute_error_cls=ctx.compute_error_cls,
            validation_error_cls=ctx.validation_error_cls,
            primary_key_missing_error_cls=PrimaryKeyMissingError,
            primary_key_null_error_cls=PrimaryKeyNullError,
            dlq_spool_error_cls=ctx.dlq_spool_error_cls,
            duckdb_module=ctx.duckdb_module,
            logger=ctx.logger,
        )

    async def _fetch_with_retry(self, job: FetchJob) -> bytes:
        """Execute a fetch job with rate limiting and exponential backoff retry.

        If `rate_limiter` is configured, it waits for a token before request.
        Retries on exceptions up to `config.retry.max_attempts`.

        Note on Rate Limiting and Pagination:
            When a single logical request (e.g. 50 tickers) is split into multiple pages by the API:
            1. The Router parses the first response and creates new FetchJobs for subsequent pages.
            2. These new jobs are fed back into the request queue.
            3. Each subsequent job MUST pass through this method again.
            4. `async with self.controller.throttle()` ensures that EVERY page request consumes a token.

            This guarantees that the physical request rate (RPM) never exceeds the limit,
            even if one "logical" user request expands into hundreds of API calls.
        """
        return await self._retry_executor.fetch(job)
