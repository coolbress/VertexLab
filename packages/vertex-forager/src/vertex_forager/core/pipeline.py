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
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
import inspect
import itertools
import logging
import os
import time
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import uuid4

import httpx
from opentelemetry import trace as otel_trace
from polars.exceptions import ComputeError
import psutil
from tqdm.auto import tqdm

from vertex_forager.constants import FLUSH_THRESHOLD_INFINITE as CONST_FLUSH_THRESHOLD_INFINITE
from vertex_forager.constants import PRIORITY_NEW_JOB as CONST_PRIORITY_NEW_JOB
from vertex_forager.constants import PRIORITY_PAGINATION as CONST_PRIORITY_PAGINATION
from vertex_forager.constants import PRIORITY_SENTINEL as CONST_PRIORITY_SENTINEL
from vertex_forager.constants import PROGRESS_LOG_CHUNK_ROWS
from vertex_forager.core.checkpoint import (
    Checkpoint,
    find_latest_checkpoint,
    get_cache_dir,
    register_dlq_entry,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.config import FetchJob, ProgressSnapshot, RunResult
from vertex_forager.core.dlq import (
    build_writer_error_summary,
    spool_to_dlq_and_rescue,
)
from vertex_forager.core.lifecycle import RunFinalizer
from vertex_forager.core.lifecycle import create_run_queues as create_run_queues_impl
from vertex_forager.core.lifecycle import create_run_result as create_run_result_impl
from vertex_forager.core.lifecycle import init_metrics_for_run as init_metrics_for_run_impl
from vertex_forager.core.lifecycle import initialize_run_state as initialize_run_state_impl
from vertex_forager.core.orchestration import await_packet_sentinel_tasks as await_packet_sentinel_tasks_impl
from vertex_forager.core.orchestration import await_writer_tasks as await_writer_tasks_impl
from vertex_forager.core.orchestration import cancel_non_writer_tasks as cancel_non_writer_tasks_impl
from vertex_forager.core.orchestration import enqueue_request_sentinels as enqueue_request_sentinels_impl
from vertex_forager.core.orchestration import schedule_packet_sentinels as schedule_packet_sentinels_impl
from vertex_forager.core.quality import validate_data_quality as validate_data_quality_impl
from vertex_forager.core.retry import RetryExecutor
from vertex_forager.core.scheduler import (
    FairnessState,
    SchedulerResult,
)
from vertex_forager.core.scheduler import (
    enqueue_pagination_job as enqueue_pagination_job_impl,
)
from vertex_forager.core.scheduler import (
    mark_pagination_job_done as mark_pagination_job_done_impl,
)
from vertex_forager.core.scheduler import (
    pop_next_job_respecting_fairness as pop_next_job_respecting_fairness_impl,
)
from vertex_forager.core.scheduler import (
    wait_for_pagination_availability as wait_for_pagination_availability_impl,
)
from vertex_forager.core.scheduler import (
    wait_for_pagination_drain as wait_for_pagination_drain_impl,
)
from vertex_forager.core.workerio import emit_packets_and_next_jobs as emit_packets_and_next_jobs_impl
from vertex_forager.core.workerio import fetch_payload as fetch_payload_impl
from vertex_forager.core.workerio import parse_payload as parse_payload_impl
from vertex_forager.core.workerio import record_worker_error as record_worker_error_impl
from vertex_forager.core.writerflush import (
    WriterContext,
    buffer_or_flush_packet,
    concat_frames_with_flex,
    flush_all_writer_buffers,
    flush_chunked_table,
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

from vertex_forager.exceptions import (
    DLQSpoolError,
    FetchError,
    PrimaryKeyMissingError,
    PrimaryKeyNullError,
    RunError,
    ValidationError,
)
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.utils import sanitize_field
from vertex_forager.writers.memory import InMemoryBufferWriter

if TYPE_CHECKING:
    import polars as pl

    from vertex_forager.core.config import FramePacket, ParseResult, ResolvedClientConfig
    from vertex_forager.core.contracts import BaseMapper, IWriter
    from vertex_forager.core.controller import FlowController
    from vertex_forager.core.http import HttpExecutor
    from vertex_forager.routers.base import BaseRouter
    from vertex_forager.schema.config import TableSchema

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


def _symbol_tokens(symbol: str | None) -> list[str]:
    if not isinstance(symbol, str) or not symbol:
        return []
    return [token for token in (part.strip() for part in symbol.split(",")) if token]


def _logical_symbol_count(job: FetchJob) -> int:
    tokens = _symbol_tokens(job.symbol)
    if not tokens:
        return 1
    return len(tokens)


def _count_completed_symbol_units(*, requested_symbols: Symbols | None, completed_symbols: set[str]) -> int:
    if not completed_symbols:
        return 0
    if requested_symbols is None:
        return sum(max(1, len(_symbol_tokens(symbol))) for symbol in completed_symbols)
    remaining: dict[str, int] = {}
    for symbol in requested_symbols:
        tokens = _symbol_tokens(symbol)
        if not tokens:
            remaining[symbol] = remaining.get(symbol, 0) + 1
            continue
        for token in tokens:
            remaining[token] = remaining.get(token, 0) + 1
    completed = 0
    for symbol in completed_symbols:
        tokens = _symbol_tokens(symbol)
        if not tokens:
            if remaining.get(symbol, 0) > 0:
                completed += 1
                remaining[symbol] -= 1
            continue
        for token in tokens:
            if remaining.get(token, 0) > 0:
                completed += 1
                remaining[token] -= 1
    return completed


def _count_requested_symbol_units(symbols: Symbols | None) -> int | None:
    if symbols is None:
        return None
    total = 0
    for symbol in symbols:
        tokens = _symbol_tokens(symbol)
        total += len(tokens) if tokens else 1
    return total


def _fsync_dir(path: str) -> None:
    try:
        dir_fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def _count_pending_request_jobs(req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] | None) -> int:
    """Count pending (non-sentinel) jobs in a request queue.

    Note: Reading req_q._queue relies on CPython implementation details and may
    break on other Python implementations. The qsize() fallback provides only an
    approximate count because it cannot filter sentinel entries (items where
    item[2] is None). Callers should treat qsize() results as best-effort.
    """
    if req_q is None:
        return 0
    queue_items = getattr(req_q, "_queue", None)
    if queue_items is None:
        qsize = getattr(req_q, "qsize", None)
        return int(qsize()) if callable(qsize) else 0
    return sum(1 for item in queue_items if len(item) >= 3 and item[2] is not None)


def _compute_window_start(
    *, progress_started_at: float, progress_history: deque[tuple[float, int]], now: float
) -> float:
    if len(progress_history) == 1:
        return progress_started_at
    if progress_history:
        return progress_history[0][0]
    return now


def _format_progress_summary(*, provider: str, dataset: str, snapshot: ProgressSnapshot) -> str:
    pct = f"{snapshot.pct:.1f}%" if snapshot.pct is not None else "n/a"
    eta = f"{snapshot.eta_s:.1f}s" if snapshot.eta_s is not None else "n/a"
    return (
        f"vertex-forager run complete | provider={provider} dataset={dataset} jobs={snapshot.jobs_done}"
        f"/{snapshot.jobs_total if snapshot.jobs_total is not None else '?'} pct={pct} "
        f"elapsed={snapshot.elapsed_s:.1f}s throughput={snapshot.throughput_sym_per_s:.2f}/s "
        f"eta={eta} rows={snapshot.rows_written} errors={snapshot.errors} retries={snapshot.retries} "
        f"throttle_events={snapshot.throttle_events} dlq_spooled={snapshot.dlq_spooled}"
    )


_tracer = otel_trace.get_tracer("vertex_forager")


class VertexForager:
    """High-performance asynchronous data pipeline engine.

    This class orchestrates the end-to-end data collection process using a
    Producer-Consumer architecture with asyncio. It manages three main stages:
    Job Generation (Producer), Data Fetching (Fetch Workers), and Data Writing
    (Writer Workers).

    Attributes:
        _router (BaseRouter): Router/Queue manager for fetch jobs.
        _http (HttpExecutor): Handles HTTP requests.
        _writer (IWriter): Writer task manager.
        _mapper (BaseMapper): Normalizes data schemas.
        _config (ResolvedClientConfig): Internal resolved configuration object.
        controller (FlowController): Rate limiter and concurrency controller.
        _flush_threshold (int): Row count threshold for flushing buffers.

    Public Methods:
        run(dataset: str, symbols: Symbols | None, ...) -> RunResult:
            Execute the full collection pipeline.
        stop() -> None:
            Gracefully stop the pipeline.
    """

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
        router: BaseRouter,
        http: HttpExecutor,
        writer: IWriter,
        mapper: BaseMapper,
        config: ResolvedClientConfig,
        controller: FlowController,
    ) -> None:
        self._router = router
        self._http = http
        self._writer = writer
        self._mapper = mapper
        self._config = config
        self.controller = controller
        self._counters: dict[str, int] = {}
        self._progress_counters: dict[str, int] = {}
        self._hists: dict[str, deque[float]] = {}

        # Track active tasks for graceful shutdown
        self._active_tasks: list[asyncio.Future[Any]] = []
        self._running = False
        self._initialize_progress_runtime()

        # Validate configuration
        self._config.assert_valid()

        # Optimization: Disable intermediate flushing for In-Memory Writer
        # Since InMemoryBufferWriter just stores frames in a list, we can
        # avoid the overhead of intermediate merges by setting threshold to infinity.
        # This allows the worker to collect ALL frames and perform a SINGLE merge at the end.
        self._flush_threshold = config.storage.flush_threshold_rows

        if InMemoryBufferWriterType is not None and isinstance(writer, InMemoryBufferWriterType):
            # Override instance config (not the global config object)
            # We treat 1 billion rows as effectively infinite for memory buffer
            self._flush_threshold = self.FLUSH_THRESHOLD_INFINITE
            logger.debug("PIPELINE: Detected InMemoryBufferWriter. Disabled intermediate flushing.")
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
        quantum = float(config.schedule.quantum)
        # Global pagination fairness bookkeeping
        self._fair_lock = asyncio.Lock()
        self._fair_state = FairnessState(
            quantum=quantum,
            max_pending_per_symbol=config.schedule.max_pending_per_symbol,
            backpressure_threshold=config.schedule.backpressure_threshold,
        )
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
        self._pending_jobs: list[FetchJob] = []
        self._checkpoint_lock = asyncio.Lock()

        pickle_compat_datasets = getattr(self._router, "pickle_compat_datasets", ())
        if not isinstance(pickle_compat_datasets, tuple):
            pickle_compat_datasets = (
                tuple(pickle_compat_datasets) if isinstance(pickle_compat_datasets, (list, set)) else ()
            )
        if pickle_compat_datasets:
            logger.warning(
                "PIPELINE: YFinance pickle compatibility enabled for datasets=%s",
                ",".join(sorted(str(dataset) for dataset in pickle_compat_datasets)),
            )
            self._inc("pickle_compat_enabled", 1)

    def _initialize_progress_runtime(self) -> None:
        self._progress_started_at = 0.0
        self._progress_total: int | None = None
        self._progress_done = 0
        self._progress_history: deque[tuple[float, int]] = deque()
        self._progress_window_events = 0
        self._active_workers = 0
        self._progress_process = psutil.Process(os.getpid())
        self._progress_process.cpu_percent(interval=None)

    def _inc(self, name: str, amount: int = 1) -> None:
        self._progress_counters[name] = self._progress_counters.get(name, 0) + amount
        self._counters[name] = self._counters.get(name, 0) + amount
        sink = self._metrics_sink
        if sink is not None:
            with suppress(Exception):
                sink.inc(name, amount)

    def _observe(self, name: str, value: float) -> None:
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

    @staticmethod
    def _job_signature(job: FetchJob) -> str:
        return job.model_dump_json()

    def _remove_pending_job(self, job: FetchJob) -> None:
        signature = self._job_signature(job)
        self._pending_jobs = [
            pending_job for pending_job in self._pending_jobs if self._job_signature(pending_job) != signature
        ]

    def _merge_pending_jobs(self, jobs: Sequence[FetchJob]) -> None:
        existing = {self._job_signature(job) for job in self._pending_jobs}
        for job in jobs:
            signature = self._job_signature(job)
            if signature in existing:
                continue
            self._pending_jobs.append(job)
            existing.add(signature)

    @contextmanager
    def _span(self, name: str, **attributes: object) -> Iterator[None]:
        with _tracer.start_as_current_span(f"vertex_forager.{name}", attributes=attributes):  # type: ignore[arg-type]
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
        logger.debug(
            "vertex_forager stage",
            extra={
                "vf_provider": sanitize_field(provider),
                "vf_dataset": sanitize_field(dataset),
                "vf_symbol": sanitize_field(symbol),
                "vf_stage": sanitize_field(stage),
                "vf_attempt": int(attempt if attempt is not None else 0),
                "vf_duration_s": None if duration_s is None else round(float(duration_s), 3),
            },
        )

    def _update_checkpoint(
        self,
        run_id: str,
        provider: str,
        dataset: str,
        table_name: str | None,
        completed_symbols: set[str],
        failed_symbols: set[str],
        pending_jobs: Sequence[FetchJob],
        status: Literal["in_progress", "completed"] = "in_progress",
    ) -> None:
        """Update checkpoint with completed and failed symbols."""
        if isinstance(self._writer, InMemoryBufferWriter):
            return
        if not completed_symbols and not failed_symbols and not pending_jobs:
            return

        checkpoint = Checkpoint(
            run_id=run_id,
            provider=provider,
            dataset=dataset,
            table_name=table_name,
            completed=list(completed_symbols),
            failed=list(failed_symbols),
            pending_jobs=list(pending_jobs),
            meta={"requested_symbols": list(getattr(self, "_requested_symbols", []))},
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

    def _snapshot_pending_jobs(self) -> list[FetchJob]:
        req_q = cast("asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] | None", getattr(self, "_req_q", None))
        jobs: list[FetchJob] = []
        seen: set[str] = set()
        for job in self._pending_jobs:
            signature = self._job_signature(job)
            if signature in seen:
                continue
            seen.add(signature)
            jobs.append(job)
        if req_q is None:
            return jobs
        for _, _, queued_job in list(getattr(req_q, "_queue", [])):
            if not isinstance(queued_job, FetchJob):
                continue
            signature = self._job_signature(queued_job)
            if signature in seen:
                continue
            seen.add(signature)
            jobs.append(queued_job)
        return jobs

    def _writer_output_uri(self) -> str | None:
        db_path = getattr(self._writer, "db_path", None)
        if isinstance(db_path, str) and db_path:
            return f"duckdb://{db_path}"
        return None

    def _resolve_checkpoint_table_name(
        self,
        *,
        table_name: str | None,
        checkpoint: Checkpoint | None,
    ) -> str:
        resolved_table_name = table_name or (checkpoint.table_name if checkpoint is not None else None)
        if resolved_table_name is None:
            raise ValueError("Pipeline run requires table_name or checkpoint.table_name")
        return resolved_table_name

    def _prepare_run_inputs(
        self,
        *,
        symbols: Symbols | None,
        table_name: str | None,
        checkpoint: Checkpoint | None,
    ) -> str:
        self._requested_symbols = list(symbols or [])
        resolved_table_name = self._resolve_checkpoint_table_name(table_name=table_name, checkpoint=checkpoint)
        self._checkpoint_table_name = resolved_table_name
        return resolved_table_name

    async def run(
        self,
        *,
        dataset: str,
        symbols: Symbols | None,
        on_progress: Callable[[ProgressSnapshot], Any] | None = None,
        progress: bool = False,
        checkpoint: Checkpoint | None = None,
        table_name: str | None = None,
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
            on_progress: Optional callback receiving ProgressSnapshot on each job completion.
            progress: Whether to show the built-in progress bar and final summary.
            checkpoint: Optional checkpoint state to resume from.
            table_name: Destination table name used for checkpoint persistence.
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
        """
        if self._running:
            raise RuntimeError("Pipeline is already running; concurrent run() calls are not supported")
        self._prepare_run_inputs(
            symbols=symbols,
            table_name=table_name,
            checkpoint=checkpoint,
        )
        self._running = True
        self._progress_started_at = 0.0
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] | None = None
        pkt_q: asyncio.Queue[FramePacket | None] | None = None
        result: RunResult | None = None
        result_lock: asyncio.Lock | None = None
        progress_bar: tqdm | None = None
        final_progress_emitted = False
        progress_runtime_initialized = False
        completed_symbols: set[str] = set()

        try:
            run_id, completed_symbols = self._initialize_run_state(dataset=dataset, checkpoint=checkpoint)
            req_q, pkt_q = self._create_run_queues()
            self._init_metrics_for_run()
            t_run0 = time.monotonic()
            result, result_lock = self._create_run_result(run_id=run_id, dataset=dataset)
            concurrency = self._resolve_concurrency()
            self._ensure_parse_executor(concurrency)
            jobs_total = _count_requested_symbol_units(symbols)
            jobs_done_initial = (
                _count_completed_symbol_units(requested_symbols=symbols, completed_symbols=completed_symbols)
                if checkpoint is not None
                else 0
            )
            self._reset_progress_runtime(jobs_total=jobs_total, jobs_done_initial=jobs_done_initial)
            progress_runtime_initialized = True
            progress_bar = (
                tqdm(total=jobs_total, initial=jobs_done_initial, unit="jobs", desc=dataset, leave=True)
                if progress
                else None
            )
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
                progress_bar=progress_bar,
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
            await self._emit_progress_snapshot(
                result=result,
                result_lock=result_lock,
                req_q=req_q,
                on_progress=on_progress,
                progress_bar=progress_bar,
                terminal_count=0,
                finished=True,
                show_summary=progress,
                summary_dataset=dataset,
            )
            final_progress_emitted = True
            return result
        finally:
            try:
                pending_jobs_before_stop = _count_pending_request_jobs(req_q)
                await self.stop()
                if not final_progress_emitted:
                    try:
                        if not progress_runtime_initialized:
                            self._reset_progress_runtime(
                                jobs_total=_count_requested_symbol_units(symbols),
                                jobs_done_initial=_count_completed_symbol_units(
                                    requested_symbols=symbols,
                                    completed_symbols=completed_symbols,
                                )
                                if checkpoint is not None
                                else 0,
                            )
                        await self._emit_progress_snapshot(
                            result=result,
                            result_lock=result_lock,
                            req_q=req_q,
                            on_progress=on_progress,
                            progress_bar=progress_bar,
                            terminal_count=0,
                            finished=True,
                            show_summary=progress,
                            summary_dataset=dataset,
                            pending_jobs_override=pending_jobs_before_stop,
                        )
                        final_progress_emitted = True
                    except Exception:
                        logger.exception("PIPELINE: Failed to emit final progress snapshot during exceptional shutdown")
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
        on_progress: Callable[[ProgressSnapshot], Any] | None,
        progress_bar: tqdm | None,
        kwargs: dict[str, object],
    ) -> tuple[asyncio.Task[Any], list[asyncio.Task[Any]], list[asyncio.Task[Any]]]:
        writer_tasks = [
            asyncio.create_task(
                self._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock),
                name=f"vertex-forager:writer:{i}",
            )
            for i in range(1)
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
                    progress_bar=progress_bar,
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
                pending_jobs=self._pending_jobs,
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
        quantum = float(self._config.schedule.quantum)
        self._fair_state = FairnessState(
            quantum=quantum,
            max_pending_per_symbol=self._config.schedule.max_pending_per_symbol,
            backpressure_threshold=self._config.schedule.backpressure_threshold,
        )
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
            with suppress(Exception):
                self._summary["req_q_len_after_producer"] = float(req_q.qsize())
                self._summary["pkt_q_len_after_producer"] = float(pkt_q.qsize())
        await req_q.join()
        await wait_for_pagination_drain_impl(fairness_state=self._fair_state)
        logger.info("PIPELINE: Request queue joined, sending sentinel signals...")
        with suppress(Exception):
            self._summary["req_q_len_after_req_join"] = float(req_q.qsize())
        for _ in range(concurrency):
            await self._put_request_queue(req_q=req_q, item=(self.PRIORITY_SENTINEL, 0, None))
        logger.info("PIPELINE: Waiting for fetch tasks to complete...")
        await asyncio.gather(*fetch_tasks)
        logger.info("PIPELINE: Fetch tasks completed, waiting for packet queue...")
        await pkt_q.join()
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

    def _initialize_run_state(self, *, dataset: str, checkpoint: Checkpoint | None) -> tuple[str, set[str]]:
        run_id, completed_symbols, failed_symbols = initialize_run_state_impl(
            provider=self._router.provider,
            dataset=dataset,
            checkpoint=checkpoint,
            logger=logger,
        )
        self._run_id = run_id
        self._completed_symbols = set(completed_symbols)
        self._failed_symbols = set(failed_symbols)
        self._pending_jobs = []
        if checkpoint is not None:
            self._merge_pending_jobs(checkpoint.pending_jobs)
        return run_id, completed_symbols

    def _create_run_queues(
        self,
    ) -> tuple[
        asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        asyncio.Queue[FramePacket | None],
    ]:
        req_q, pkt_q = create_run_queues_impl(
            queue_max=self._config.queue_max,
            checkpoint_retention_days=self._config.storage.checkpoint_retention_days,
            run_history_retention_days=self._config.storage.run_history_retention_days,
            dlq_tmp_retention_s=self._config.storage.dlq_tmp_retention_s,
            cache_dir=get_cache_dir(),
            logger=logger,
        )
        return (
            cast("asyncio.PriorityQueue[tuple[int, int, FetchJob | None]]", req_q),
            cast("asyncio.Queue[FramePacket | None]", pkt_q),
        )

    def _init_metrics_for_run(self) -> None:
        counters, hists, summary = init_metrics_for_run_impl()
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
        self._merge_component_counters()
        result.metrics_counters = dict(self._counters)
        result.metrics_histograms = {k: list(v) for k, v in self._hists.items()}
        self._summary.update(self._compute_summary())
        result.metrics_summary = dict(self._summary)
        sink = self._metrics_sink
        if sink is not None:
            with suppress(Exception):
                sink.summary(dict(self._summary))
        self._emit_pipeline_summary_log(dataset=dataset, started_monotonic=started_monotonic)
        result.finished_at = time.time()
        result.duration_s = time.monotonic() - started_monotonic
        async with self._checkpoint_lock:
            await asyncio.to_thread(
                self._update_checkpoint,
                run_id,
                self._router.provider,
                dataset,
                getattr(self, "_checkpoint_table_name", None),
                self._completed_symbols,
                self._failed_symbols,
                self._snapshot_pending_jobs(),
                "completed" if not self._failed_symbols and not self._pending_jobs else "in_progress",
            )
        try:
            save_run_history(result, run_id, table_name=self._checkpoint_table_name)
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
        pending_jobs: Sequence[FetchJob] | None = None,
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

        requested_symbols = set(symbols) if symbols else None
        completed_symbol_tokens = {token for symbol in (completed_symbols or set()) for token in _symbol_tokens(symbol)}
        filtered_pending_jobs = [
            pending_job
            for pending_job in (pending_jobs or [])
            if requested_symbols is None
            or (pending_job.symbol is None and requested_symbols is None)
            or pending_job.symbol in requested_symbols
        ]
        pending_job_signatures = {self._job_signature(pending_job) for pending_job in filtered_pending_jobs}
        pending_symbols = {
            pending_job.symbol for pending_job in filtered_pending_jobs if pending_job.symbol is not None
        }
        pending_token_sets = [
            set(_symbol_tokens(pending_job.symbol))
            for pending_job in filtered_pending_jobs
            if _symbol_tokens(pending_job.symbol)
        ]
        for pending_job in filtered_pending_jobs:
            await enqueue_pagination_job_impl(
                fair_lock=self._fair_lock,
                fairness_state=self._fair_state,
                job=pending_job,
            )

        async for job in self._router.generate_jobs(dataset=dataset, symbols=symbols, **kwargs):
            # Skip already completed symbols if resume is enabled
            if completed_symbols and job.symbol:
                job_tokens = _symbol_tokens(job.symbol)
                if job.symbol in completed_symbols or (
                    job_tokens and all(token in completed_symbol_tokens for token in job_tokens)
                ):
                    logger.debug("PRODUCER: Skipping already completed symbol %s", job.symbol)
                    continue
            if self._job_signature(job) in pending_job_signatures:
                logger.debug("PRODUCER: Skipping duplicate pending checkpoint job %s", job)
                continue
            job_token_set = set(_symbol_tokens(job.symbol)) if job.symbol else set()
            if job.symbol and (job.symbol in pending_symbols or job_token_set in pending_token_sets):
                logger.debug("PRODUCER: Resuming paginated symbol %s from pending checkpoint job", job.symbol)
                continue

            await self._put_request_queue(req_q=req_q, item=(self.PRIORITY_NEW_JOB, next(order_counter), job))
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
                try:
                    dlq_dir = get_cache_dir() / "dlq" / pkt.table
                    dlq_dir.mkdir(parents=True, exist_ok=True)
                    ts_ns = time.time_ns()
                    file_id = f"{ts_ns}_{uuid4().hex}"
                    fpath = dlq_dir / f"packet_{file_id}.ipc"
                    tmp_path = fpath.parent / f"{fpath.name}.tmp"
                    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                    with os.fdopen(fd, "wb") as fh:
                        pkt.frame.write_ipc(fh)
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.replace(tmp_path, fpath)
                    _fsync_dir(str(dlq_dir))
                    try:
                        register_dlq_entry(
                            path=fpath,
                            table=pkt.table,
                            provider=pkt.provider,
                            row_count=len(pkt.frame),
                            output_uri=self._writer_output_uri(),
                        )
                    except Exception as reg_err:
                        with suppress(OSError):
                            fpath.unlink()
                            _fsync_dir(str(dlq_dir))
                        raise reg_err
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
                        self._inc("dlq_remaining_total", 1)
                        self._inc(f"dlq_remaining.{pkt.table}", 1)
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
        on_progress: Callable[[ProgressSnapshot], Any] | None = None,
        progress_bar: tqdm | None = None,
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
        job_count = 0
        while True:
            selected = await self._dequeue_worker_job(req_q=req_q)
            priority = selected.priority
            job = selected.job
            already_done = selected.already_done
            if self._should_exit_worker_for_sentinel(
                job=job,
                priority=priority,
                already_done=already_done,
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
            worker_exc: BaseException | None = None
            parse_result: ParseResult | None = None
            try:
                self._active_workers += 1
                _payload, worker_exc, parse_result = await self._process_worker_job(
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
                if priority == self.PRIORITY_PAGINATION:
                    await mark_pagination_job_done_impl(
                        fair_lock=self._fair_lock,
                        fairness_state=self._fair_state,
                    )
                else:
                    req_q.task_done()
                try:
                    self._active_workers = max(0, self._active_workers - 1)
                    await self._record_worker_symbol_state(job=job, worker_exc=worker_exc, parse_result=parse_result)
                    terminal_count = 0
                    next_jobs = list(parse_result.next_jobs if parse_result is not None else [])
                    if (worker_exc is not None and not isinstance(worker_exc, asyncio.CancelledError)) or (
                        worker_exc is None and not next_jobs
                    ):
                        terminal_count = _logical_symbol_count(job)
                    elif job.symbol is None and next_jobs:
                        terminal_count = 1
                    await self._emit_progress_snapshot(
                        result=result,
                        result_lock=result_lock,
                        req_q=req_q,
                        on_progress=on_progress,
                        progress_bar=progress_bar,
                        terminal_count=terminal_count,
                        finished=False,
                        show_summary=False,
                        summary_dataset=job.dataset,
                    )
                except Exception as e:
                    logger.error(
                        "[Worker-%s] Error updating progress state for %s:%s:%s: %s",
                        worker_id,
                        job.provider,
                        job.dataset,
                        job.symbol,
                        e,
                    )

    def _reset_progress_runtime(self, *, jobs_total: int | None, jobs_done_initial: int = 0) -> None:
        self._progress_started_at = time.monotonic()
        self._progress_total = jobs_total
        self._progress_done = jobs_done_initial
        self._progress_history.clear()
        self._progress_window_events = 0
        self._active_workers = 0
        self._progress_display_done = jobs_done_initial
        self._progress_counters = {}
        self._progress_process.cpu_percent(interval=None)

    def _update_progress_bar(self, *, progress_bar: tqdm | None, snapshot: ProgressSnapshot) -> None:
        if progress_bar is None:
            return
        delta = max(0, snapshot.jobs_done - self._progress_display_done)
        if delta:
            progress_bar.update(delta)
            self._progress_display_done += delta
        progress_bar.set_postfix(
            throughput=f"{snapshot.throughput_sym_per_s:.2f}/s",
            eta="n/a" if snapshot.eta_s is None else f"{snapshot.eta_s:.1f}s",
            errors=snapshot.errors,
            rows=snapshot.rows_written,
            refresh=False,
        )
        if snapshot.finished:
            progress_bar.close()

    async def _emit_progress_snapshot(
        self,
        *,
        result: RunResult | None,
        result_lock: asyncio.Lock | None,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] | None,
        on_progress: Callable[[ProgressSnapshot], Any] | None,
        progress_bar: tqdm | None,
        terminal_count: int,
        finished: bool,
        show_summary: bool,
        summary_dataset: str | None,
        pending_jobs_override: int | None = None,
    ) -> None:
        now = time.monotonic()
        if terminal_count > 0:
            self._progress_done += terminal_count
            self._progress_history.append((now, terminal_count))
            self._progress_window_events += terminal_count
        cutoff = now - 30.0
        while self._progress_history and self._progress_history[0][0] < cutoff:
            _ts, count = self._progress_history.popleft()
            self._progress_window_events = max(0, self._progress_window_events - count)
        if not finished and progress_bar is None and on_progress is None:
            return
        elapsed_s = max(0.0, now - self._progress_started_at)
        window_events = self._progress_window_events
        window_start = _compute_window_start(
            progress_started_at=self._progress_started_at,
            progress_history=self._progress_history,
            now=now,
        )
        window_span = min(30.0, max(0.0, now - window_start)) if self._progress_history else 0.0
        throughput = float(window_events / window_span) if window_span > 0.0 else 0.0
        jobs_total = self._progress_total
        if jobs_total not in (None, 0):
            known_jobs_total = cast("int", jobs_total)
            pct = float((self._progress_done / known_jobs_total) * 100.0)
            eta_s = float(max(0, known_jobs_total - self._progress_done) / throughput) if throughput > 0.0 else None
        else:
            pct = None
            eta_s = None
        if result is None:
            result = RunResult(provider=self._router.provider, dataset=None)
        if result_lock is None:
            result_lock = asyncio.Lock()
        async with result_lock:
            errors = len(result.errors)
        pending_jobs = (
            pending_jobs_override if pending_jobs_override is not None else _count_pending_request_jobs(req_q)
        )
        snapshot = ProgressSnapshot(
            jobs_done=self._progress_done,
            jobs_total=jobs_total,
            pct=pct,
            throughput_sym_per_s=throughput,
            eta_s=eta_s,
            errors=errors,
            retries=int(getattr(self.controller, "retry_events", 0)),
            rows_written=int(self._progress_counters.get("rows_written_total", 0)),
            elapsed_s=float(result.duration_s) if finished and result.duration_s is not None else elapsed_s,
            active_workers=self._active_workers,
            pending_jobs=pending_jobs,
            throttle_events=int(getattr(self.controller, "throttle_events", 0)),
            dlq_spooled=int(self._progress_counters.get("dlq_spooled_files_total", 0)),
            memory_mb=float(self._progress_process.memory_info().rss / (1024 * 1024)),
            cpu_pct=float(self._progress_process.cpu_percent(interval=None)),
            finished=finished,
        )
        self._update_progress_bar(progress_bar=progress_bar, snapshot=snapshot)
        if on_progress is None:
            if finished and show_summary:
                print(
                    _format_progress_summary(
                        provider=self._router.provider,
                        dataset=summary_dataset or result.dataset or "",
                        snapshot=snapshot,
                    )
                )
            return
        try:
            maybe_awaitable = on_progress(snapshot)
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception as e:
            logger.error("Error in on_progress callback: %s", e)
        if finished and show_summary:
            print(
                _format_progress_summary(
                    provider=self._router.provider,
                    dataset=summary_dataset or result.dataset or "",
                    snapshot=snapshot,
                )
            )

    def _drain_deferred_demotes(self, **_: object) -> None:
        return None

    def _requeue_demoted_jobs(self, **_: object) -> None:
        return None

    async def _put_request_queue(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        item: tuple[int, int, FetchJob | None],
    ) -> None:
        await req_q.put(item)

    async def _get_request_queue(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    ) -> tuple[int, int, FetchJob | None]:
        return await req_q.get()

    def _try_get_request_queue_nowait(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    ) -> tuple[int, int, FetchJob | None] | None:
        try:
            return req_q.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _dequeue_worker_job(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    ) -> SchedulerResult:
        async def _cancel_pending(tasks: tuple[asyncio.Task[object], ...]) -> None:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with suppress(asyncio.CancelledError):
                    await task

        while True:
            request_item = self._try_get_request_queue_nowait(req_q=req_q)
            if request_item is not None:
                priority, _, job = request_item
                already_done = False
                if job is None:
                    req_q.task_done()
                    already_done = True
                return SchedulerResult(priority=priority, job=job, demoted=[], already_done=already_done)
            selected = await pop_next_job_respecting_fairness_impl(
                fair_lock=self._fair_lock,
                priority_pagination=self.PRIORITY_PAGINATION,
                fairness_state=self._fair_state,
            )
            if selected.job is not None:
                return selected
            pagination_wait = asyncio.create_task(
                wait_for_pagination_availability_impl(fairness_state=self._fair_state)
            )
            req_get = asyncio.create_task(self._get_request_queue(req_q=req_q))
            pending_tasks = (pagination_wait, req_get)
            try:
                done, pending = await asyncio.wait({pagination_wait, req_get}, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                await _cancel_pending(pending_tasks)
                raise
            await _cancel_pending(tuple(pending))
            if req_get in done:
                priority, _, job = req_get.result()
                already_done = False
                if job is None:
                    req_q.task_done()
                    already_done = True
                return SchedulerResult(priority=priority, job=job, demoted=[], already_done=already_done)
            if pagination_wait in done:
                continue

    def _should_exit_worker_for_sentinel(
        self,
        *,
        job: FetchJob | None,
        priority: int,
        already_done: bool,
        worker_id: int,
        job_count: int,
    ) -> bool:
        if job is not None:
            return False
        if not (already_done and priority == self.PRIORITY_SENTINEL):
            return False
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
        async def enqueue_pagination_job(job_to_queue: FetchJob) -> None:
            await enqueue_pagination_job_impl(
                fair_lock=self._fair_lock,
                fairness_state=self._fair_state,
                job=job_to_queue,
            )

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
            enqueue_pagination_job=enqueue_pagination_job,
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
        async with self._checkpoint_lock:
            self._remove_pending_job(job)
            if worker_exc is None:
                pending_jobs = list(parse_result.next_jobs if parse_result is not None else [])
                if pending_jobs:
                    if job.symbol:
                        self._completed_symbols.discard(job.symbol)
                        self._failed_symbols.discard(job.symbol)
                    self._merge_pending_jobs(pending_jobs)
                else:
                    if job.symbol:
                        self._failed_symbols.discard(job.symbol)
                        self._completed_symbols.add(job.symbol)
            else:
                self._merge_pending_jobs([job])
                if job.symbol:
                    self._completed_symbols.discard(job.symbol)
                    self._failed_symbols.add(job.symbol)
            if self._run_id and job.dataset:
                await asyncio.to_thread(
                    self._update_checkpoint,
                    self._run_id,
                    self._router.provider,
                    job.dataset,
                    getattr(self, "_checkpoint_table_name", None),
                    self._completed_symbols,
                    self._failed_symbols,
                    self._snapshot_pending_jobs(),
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
            quality_check=self._config.quality_check,
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
            schema: TableSchema | None,
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
            schema: TableSchema | None,
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

        await flush_writer_table(
            table=table,
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
            get_table_schema=ctx.get_table_schema,
            flush_chunked_table=_flush_chunked_table,
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
