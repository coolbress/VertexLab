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
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from functools import partial
import inspect
import itertools
import logging
import os
import re
import time
from typing import TYPE_CHECKING, Any, cast

import httpx
import polars as pl
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
    get_cache_dir,
    load_checkpoint,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.dlq import (
    build_writer_error_summary,
    spool_to_dlq_and_rescue,
)
from vertex_forager.exceptions import (
    DLQSpoolError,
    FetchError,
    PrimaryKeyMissingError,
    PrimaryKeyNullError,
    ValidationError,
)

try:
    import duckdb as _duckdb
except ImportError:
    _duckdb = cast("Any", None)

from vertex_forager.core.config import (
    EngineConfig,
    FetchJob,
    FramePacket,
    ParseResult,
    RunResult,
)
from vertex_forager.core.errors import RunError
from vertex_forager.core.retry import create_retry_controller
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.utils import cleanup_dlq_tmp, sanitize_field

if TYPE_CHECKING:
    from vertex_forager.core.contracts import IMapper, IRouter, IWriter
    from vertex_forager.core.controller import FlowController
    from vertex_forager.core.http import HttpExecutor
    from vertex_forager.core.types import DLQStatus
    from vertex_forager.schema.config import TableSchema

InMemoryBufferWriterType: type | None
try:
    import vertex_forager.writers.memory as _mem_writer

    InMemoryBufferWriterType = _mem_writer.InMemoryBufferWriter
except (ImportError, ModuleNotFoundError):
    InMemoryBufferWriterType = None

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
        _config (EngineConfig): Configuration object.
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
        config: EngineConfig,
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
        # Optional tracing hooks (no hard dependency)
        self._tracer = getattr(config, "tracer", None)
        self._otel_enabled = bool(
            getattr(config, "otel_enabled", False)
            or str(os.getenv("VF_OTEL_ENABLED", "")).lower() in {"1", "true", "yes"}
        )

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
        self._fair_lock: asyncio.Lock | None = None
        self._fair_last_symbol: str | None = None
        self._fair_burst_count: int = 0
        # Writer flush idempotence
        self._writer_flushed: bool = False
        self._writer_flush_attempted: bool = False
        self._summary: dict[str, float] = {}
        self._metrics_sink = getattr(config, "metrics_sink", None)
        self._stop_task: asyncio.Task[None] | None = None

        # For checkpoint tracking
        self._completed_symbols: set[str] = set()
        self._failed_symbols: set[str] = set()
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

    def _update_checkpoint(self, run_id: str, provider: str, dataset: str,
                          completed_symbols: set[str], failed_symbols: set[str]) -> None:
        """Update checkpoint with completed and failed symbols."""
        if not completed_symbols and not failed_symbols:
            return

        checkpoint = Checkpoint(
            run_id=run_id,
            provider=provider,
            dataset=dataset,
            completed=list(completed_symbols),
            failed=list(failed_symbols)
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
        cache_dir = get_cache_dir()
        checkpoints_dir = cache_dir / "checkpoints"

        if not checkpoints_dir.exists():
            return None

        latest_checkpoint = None
        latest_mtime = 0.0

        # Look for checkpoint directories that match the provider and dataset pattern
        for checkpoint_dir in checkpoints_dir.iterdir():
            if not checkpoint_dir.is_dir():
                continue

            # Check if this checkpoint matches our provider and dataset
            checkpoint_file = checkpoint_dir / "progress.json"
            if checkpoint_file.exists():
                try:
                    checkpoint = load_checkpoint(checkpoint_dir.name)
                    if (checkpoint and checkpoint.provider == provider
                        and checkpoint.dataset == dataset):
                        # Get modification time to find the latest
                        mtime = checkpoint_file.stat().st_mtime
                        if mtime > latest_mtime:
                            latest_mtime = mtime
                            latest_checkpoint = checkpoint
                except Exception as e:
                    # Skip corrupted checkpoint files
                    logger.debug("PIPELINE: Skipping corrupted checkpoint %s: %s", checkpoint_dir.name, e)
                    continue

        return latest_checkpoint

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
              based on checkpoint files in ~/.cache/vertex-forager/checkpoints/<run_id>/.
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
                name="vertex-forager:writer:0",
            )
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
        self._fair_lock = asyncio.Lock()
        self._fair_last_symbol = None
        self._fair_burst_count = 0
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
        run_id = f"{self._router.provider}_{dataset}_{int(time.time())}"
        completed_symbols: set[str] = set()
        self._run_id = run_id
        self._completed_symbols = set()
        self._failed_symbols = set()
        if not resume:
            return run_id, completed_symbols
        latest_checkpoint = self._find_latest_checkpoint(self._router.provider, dataset)
        if latest_checkpoint is None:
            logger.info(
                "PIPELINE: No checkpoint found for provider %s and dataset %s, starting fresh",
                self._router.provider,
                dataset,
            )
            return run_id, completed_symbols
        run_id = latest_checkpoint.run_id
        completed_symbols = set(latest_checkpoint.completed)
        self._run_id = run_id
        self._completed_symbols = set(latest_checkpoint.completed)
        self._failed_symbols = set(latest_checkpoint.failed)
        logger.info(
            "PIPELINE: Resuming from checkpoint %s, skipping %d completed symbols",
            run_id,
            len(completed_symbols),
        )
        return run_id, completed_symbols

    def _create_run_queues(
        self,
    ) -> tuple[
        asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        asyncio.Queue[FramePacket | None],
    ]:
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue(
            maxsize=self._config.queue_max
        )
        pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue(maxsize=self._config.queue_max)
        if getattr(self._config, "dlq_tmp_periodic_cleanup", False):
            try:
                cleanup_dlq_tmp(get_cache_dir() / "dlq", int(getattr(self._config, "dlq_tmp_retention_s", 86400)))
            except Exception as cleanup_error:
                logger.warning("PIPELINE: DLQ periodic cleanup failed: %s", cleanup_error)
        return req_q, pkt_q

    def _init_metrics_for_run(self) -> None:
        if not self._metrics_enabled:
            return
        self._counters = {}
        self._hists = {}
        self._summary = {}
        self._counters["pipeline_runs"] = 1

    def _create_run_result(self, *, run_id: str, dataset: str) -> tuple[RunResult, asyncio.Lock]:
        result = RunResult(provider=self._router.provider)
        result.run_id = run_id
        result.dataset = dataset
        result.started_at = time.time()
        result_lock = asyncio.Lock()
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
            )
        if self._config.persist_run_history:
            try:
                save_run_history(result, run_id)
                logger.debug("PIPELINE: Run history saved for run %s", run_id)
            except Exception as e:
                logger.warning("PIPELINE: Failed to save run history: %s", e)

    def _merge_component_counters(self) -> None:
        with suppress(Exception):
            mapper_counters = getattr(self._mapper, "get_counters_and_reset", None)
            if callable(mapper_counters):
                for k, v in dict(mapper_counters()).items():
                    self._inc(k, int(v))
        with suppress(Exception):
            writer_counters = getattr(self._writer, "get_counters_and_reset", None)
            if callable(writer_counters):
                for k, v in dict(writer_counters()).items():
                    self._inc(k, int(v))

    def _emit_pipeline_summary_log(self, *, dataset: str, started_monotonic: float) -> None:
        if not self._structured_logs:
            return
        dur_run = time.monotonic() - started_monotonic
        msg_s = (
            f"OBS provider={sanitize_field(self._router.provider)} "
            f"dataset={sanitize_field(dataset)} symbol=* stage=pipeline_summary attempt=0 "
            f"duration={dur_run:.3f}s " + " ".join(f"{k}={v:.3f}" for k, v in sorted(self._summary.items()))
        )
        if self._log_verbose:
            logger.info(msg_s)
        else:
            logger.debug(msg_s)

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
        writer_tasks = getattr(self, "_writer_tasks", None)
        writer_set: set[asyncio.Task[Any]] = (
            set(cast("list[asyncio.Task[Any]]", writer_tasks)) if isinstance(writer_tasks, list) else set()
        )
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
        other_tasks: list[asyncio.Task[Any]] = [
            t for t in cast("list[asyncio.Task[Any]]", self._active_tasks) if t not in writer_set
        ]
        for task in other_tasks:
            if not task.done():
                task.cancel()
        if other_tasks:
            await asyncio.gather(*other_tasks, return_exceptions=True)

    async def _enqueue_request_sentinels(self) -> None:
        try:
            req_q = getattr(self, "_req_q", None)
            fetch_n = int(getattr(self.controller, "concurrency_limit", 0) or 0)
            if req_q is None or fetch_n <= 0:
                return
            for _ in range(fetch_n):
                try:
                    req_q.put_nowait((self.PRIORITY_SENTINEL, 0, None))
                except Exception as e:
                    logger.debug("PIPELINE: Skipping req_q sentinel (%s)", e)
                    break
        except Exception as e:
            logger.debug("PIPELINE: Failed to enqueue request sentinels during stop: %s", e, exc_info=True)

    def _schedule_packet_sentinels(self, writer_tasks: object) -> tuple[int, list[asyncio.Task[Any]]]:
        try:
            pkt_q = getattr(self, "_pkt_q", None)
            if pkt_q is None:
                return 0, []
            active_writers = 0
            if isinstance(writer_tasks, list):
                try:
                    active_writers = sum(1 for t in writer_tasks if isinstance(t, asyncio.Task) and not t.done())
                except Exception:
                    active_writers = len(writer_tasks)
            sentinel_put_tasks: list[asyncio.Task[Any]] = []
            if active_writers <= 0:
                return 0, []
            remaining = active_writers
            for _ in range(active_writers):
                try:
                    pkt_q.put_nowait(None)
                    remaining -= 1
                except Exception as e:
                    logger.debug("PIPELINE: pkt_q full when enqueuing sentinel (%s), scheduling async puts", e)
                    break
            for _ in range(remaining):
                try:
                    sentinel_put_tasks.append(asyncio.create_task(pkt_q.put(None)))
                except Exception as e:
                    logger.debug("PIPELINE: Failed to schedule pkt_q.put(None) task: %s", e)
            return active_writers, sentinel_put_tasks
        except Exception as e:
            logger.debug("PIPELINE: Failed to enqueue packet sentinels during stop: %s", e, exc_info=True)
            return 0, []

    async def _await_packet_sentinel_tasks(
        self,
        *,
        writer_set: set[asyncio.Task[Any]],
        sentinel_put_tasks: list[asyncio.Task[Any]],
    ) -> bool:
        if not sentinel_put_tasks:
            return False
        try:
            await asyncio.wait_for(asyncio.gather(*sentinel_put_tasks, return_exceptions=True), timeout=10.0)
            return False
        except asyncio.TimeoutError:
            for t in sentinel_put_tasks:
                with suppress(Exception):
                    t.cancel()
            logger.debug("PIPELINE: Timeout awaiting pkt_q sentinel put tasks; cancelled pending puts")
            if writer_set:
                for w in list(writer_set):
                    with suppress(Exception):
                        w.cancel()
                with suppress(Exception):
                    await asyncio.gather(*writer_set, return_exceptions=True)
            return True

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
        if not writer_set:
            return
        try:
            await asyncio.gather(*writer_set, return_exceptions=True)
        except Exception:
            logger.debug("PIPELINE: Awaiting writer tasks raised but suppressed", exc_info=True)

    async def _producer(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        dataset: str,
        symbols: Symbols | None,
        order_counter: itertools.count,
        completed_symbols: set[str] | None = None,
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

        async for job in self._router.generate_jobs(dataset=dataset, symbols=symbols, **kwargs):
            # Skip already completed symbols if resume is enabled
            if completed_symbols and job.symbol and job.symbol in completed_symbols:
                logger.debug("PRODUCER: Skipping already completed symbol %s", job.symbol)
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

    async def _pop_next_job_respecting_fairness(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        burst_cap: int | None,
    ) -> tuple[int, FetchJob | None, list[FetchJob], bool]:
        """Atomically select the next job while enforcing pagination burst cap.

        Returns:
            priority: Selected job priority.
            job: Selected FetchJob (None when sentinel was consumed).
            demote_jobs: Jobs to requeue at lower priority (processed outside the lock).
            already_done: True if this method already called task_done on the selected item
                          (only true when consuming a sentinel); caller must NOT call task_done again.
        """
        demote_jobs: list[FetchJob] = []
        already_done = False
        fair_lock = self._fair_lock
        if fair_lock is None:
            raise RuntimeError("Fairness lock must be initialized before use")
        async with fair_lock:
            while True:
                priority, _, job = await req_q.get()
                # Sentinel: consume and return
                if job is None:
                    req_q.task_done()
                    already_done = True
                    return priority, None, demote_jobs, already_done
                # Non-pagination or no cap: establish baseline fairness state and return
                if burst_cap is None or priority != self.PRIORITY_PAGINATION:
                    self._fair_last_symbol = None if priority != self.PRIORITY_PAGINATION else job.symbol
                    self._fair_burst_count = 0 if priority != self.PRIORITY_PAGINATION else 1
                    return priority, job, demote_jobs, already_done
                # Pagination with cap
                if self._fair_last_symbol == job.symbol:
                    self._fair_burst_count += 1
                else:
                    self._fair_last_symbol = job.symbol
                    self._fair_burst_count = 1
                if self._fair_burst_count <= burst_cap:
                    return priority, job, demote_jobs, already_done
                # Cap exceeded: demote this job and any consecutive same-symbol paginations at the head
                demote_jobs.append(job)
                return self._pick_after_demotion(
                    req_q=req_q,
                    demote_jobs=demote_jobs,
                    already_done=already_done,
                )

    def _pick_after_demotion(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        demote_jobs: list[FetchJob],
        already_done: bool,
    ) -> tuple[int, FetchJob | None, list[FetchJob], bool]:
        while True:
            try:
                p2, _, cand = req_q.get_nowait()
            except asyncio.QueueEmpty:
                return self.PRIORITY_NEW_JOB, None, demote_jobs, already_done
            if p2 == self.PRIORITY_PAGINATION and cand is not None and cand.symbol == self._fair_last_symbol:
                demote_jobs.append(cand)
                continue
            if cand is None:
                req_q.task_done()
                already_done = True
                return p2, None, demote_jobs, already_done
            if p2 == self.PRIORITY_PAGINATION:
                self._fair_last_symbol = cand.symbol
                self._fair_burst_count = 1
            else:
                self._fair_last_symbol = None
                self._fair_burst_count = 0
            return p2, cand, demote_jobs, already_done

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
            priority, job, demote_jobs, already_done = await self._dequeue_worker_job(
                req_q=req_q,
                burst_cap=burst_cap,
            )
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
            finally:
                req_q.task_done()
                try:
                    await handler(job, payload, worker_exc, parse_result)
                    await self._record_worker_symbol_state(job=job, worker_exc=worker_exc)
                except Exception as e:
                    logger.error(
                        "[Worker-%s] Error in result handler for %s:%s:%s: %s",
                        worker_id,
                        job.provider,
                        job.dataset,
                        job.symbol,
                        e,
                    )

    def _build_progress_handler(
        self,
        on_progress: Callable[..., Any] | None,
    ) -> Callable[[FetchJob, bytes | None, Exception | None, ParseResult | None], Awaitable[None]]:
        async def noop_handler(
            job: FetchJob,
            payload: bytes | None,
            exc: Exception | None,
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
            exc: Exception | None,
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
    ) -> tuple[int, FetchJob | None, list[FetchJob], bool]:
        if burst_cap is None:
            priority, _, job = await req_q.get()
            already_done = False
            if job is None:
                req_q.task_done()
                already_done = True
            return priority, job, [], already_done
        return await self._pop_next_job_respecting_fairness(req_q=req_q, burst_cap=burst_cap)

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
    ) -> tuple[bytes | None, Exception | None, ParseResult | None]:
        payload: bytes | None = None
        worker_exc: Exception | None = None
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
        logger.debug("[Worker-%s] Processing job: %s (priority: %s)", worker_id, job.symbol, priority)
        self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="fetch_start")
        t_fetch_start = time.monotonic()
        with self._span("fetch", provider=job.provider, dataset=job.dataset, symbol=str(job.symbol)):
            payload = await self._fetch_with_retry(job)
        fetch_latency = time.monotonic() - t_fetch_start
        self._observe("fetch_duration_s", fetch_latency)
        self._log_structured(
            provider=job.provider,
            dataset=job.dataset,
            symbol=job.symbol,
            stage="fetch_end",
            duration_s=fetch_latency,
        )
        logger.debug(
            "[Worker-%s] Fetched %s (%s bytes) in %.3fs",
            worker_id,
            job.symbol,
            len(payload) if payload else 0,
            fetch_latency,
        )
        return payload

    async def _parse_payload(self, *, job: FetchJob, payload: bytes, worker_id: int) -> ParseResult:
        self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="parse_start")
        t1 = time.monotonic()
        loop = asyncio.get_running_loop()
        with self._span("parse", provider=job.provider, dataset=job.dataset, symbol=str(job.symbol)):
            parse_result = await loop.run_in_executor(
                self._parse_executor,
                partial(self._router.parse, job=job, payload=payload),
            )
        parse_latency = time.monotonic() - t1
        self._observe("parse_duration_s", parse_latency)
        logger.debug(
            "[Worker-%s] Parsed %s in %.3fs. Packets: %s, Next Jobs: %s",
            worker_id,
            job.symbol,
            parse_latency,
            len(parse_result.packets),
            len(parse_result.next_jobs),
        )
        self._log_structured(
            provider=job.provider,
            dataset=job.dataset,
            symbol=job.symbol,
            stage="parse_end",
            duration_s=parse_latency,
        )
        return parse_result

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
        for packet in parse_result.packets:
            loop = asyncio.get_running_loop()
            normalized_packet = await loop.run_in_executor(
                self._parse_executor,
                partial(self._mapper.normalize, packet=packet),
            )
            await pkt_q.put(normalized_packet)
            self._inc("packets_emitted", 1)
        if parse_result.next_jobs:
            logger.debug(
                "[Worker-%s] Adding %s pagination jobs for %s",
                worker_id,
                len(parse_result.next_jobs),
                job.symbol,
            )
            for next_job in parse_result.next_jobs:
                await req_q.put((self.PRIORITY_PAGINATION, next(order_counter), next_job))

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
        async with result_lock:
            result.errors.append(RunError.from_exception(exc, job.provider, job.dataset, job.symbol))
        if stage == "error_unexpected":
            logger.exception("[Worker-%s] Unexpected error processing %s: %s", worker_id, job.symbol, exc)
        elif stage == "error_fetch":
            logger.error("[Worker-%s] Fetch exhausted for %s: %s", worker_id, job.symbol, exc)
        else:
            logger.error("[Worker-%s] Error processing %s: %s", worker_id, job.symbol, exc)
        self._inc("errors_total", 1)
        self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage=stage)

    async def _record_worker_symbol_state(self, *, job: FetchJob, worker_exc: Exception | None) -> None:
        if not job.symbol:
            return
        async with self._checkpoint_lock:
            if worker_exc is None:
                self._completed_symbols.add(job.symbol)
            else:
                self._failed_symbols.add(job.symbol)
            if self._run_id and job.dataset:
                self._update_checkpoint(
                    self._run_id,
                    self._router.provider,
                    job.dataset,
                    self._completed_symbols,
                    self._failed_symbols,
                )

    async def _validate_data_quality(
        self,
        *,
        table: str,
        df: pl.DataFrame,
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        """Validate data quality using table schema rules.

        Args:
            table: Table name to validate
            df: DataFrame to validate
            result: RunResult to record violations
            result_lock: Lock for thread-safe result updates
        """
        schema = get_table_schema(table)
        if not schema or not schema.quality_rules:
            return

        violations_count = 0

        def _parse_violation_count(message: str) -> int:
            contains_match = re.search(r"\bcontains\s+(\d+)\b", message, flags=re.IGNORECASE)
            if contains_match is not None:
                return int(contains_match.group(1))
            found_match = re.search(r"\bfound\s+(\d+)\b", message, flags=re.IGNORECASE)
            if found_match is not None:
                return int(found_match.group(1))
            return 1

        for rule in schema.quality_rules:
            try:
                violation_messages = rule.validate(df)
                if violation_messages:
                    for violation_message in violation_messages:
                        violations_count += _parse_violation_count(violation_message)
                    logger.warning(
                        "Data quality violations in table %s: %s",
                        table,
                        ", ".join(violation_messages),
                    )
            except Exception as e:
                logger.warning(
                    "Failed to execute quality rule %s for table %s: %s",
                    type(rule).__name__,
                    table,
                    e,
                )

        if violations_count > 0:
            async with result_lock:
                result.add_quality_violations(table=table, count=violations_count)

    async def _writer_worker(
        self,
        *,
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        buffers: dict[str, list[FramePacket]] = defaultdict(list)
        buffer_rows: dict[str, int] = defaultdict(int)
        threshold = self._flush_threshold
        logger.debug("WRITER: Adaptive bulk writing enabled. Threshold=%s rows", threshold)
        while True:
            got_item = False
            try:
                packet = await pkt_q.get()
                got_item = True
                if packet is None:
                    logger.debug("WRITER: Received shutdown signal. Flushing remaining buffers...")
                    await self._flush_all_writer_buffers(
                        buffers=buffers,
                        buffer_rows=buffer_rows,
                        result=result,
                        result_lock=result_lock,
                    )
                    return
                await self._buffer_or_flush_packet(
                    packet=packet,
                    threshold=threshold,
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
            except asyncio.CancelledError:
                await self._flush_on_writer_cancel(
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
                raise
            except Exception as e:
                async with result_lock:
                    if not getattr(e, "_already_reported", False):
                        result.errors.append(RunError.from_exception(
                            exc=e,
                            provider="",
                            dataset="",
                            symbol=""
                        ))
                logger.exception("WRITER: Unexpected error")
                raise
            finally:
                if got_item:
                    pkt_q.task_done()

    async def _flush_on_writer_cancel(
        self,
        *,
        buffers: dict[str, list[FramePacket]],
        buffer_rows: dict[str, int],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        for table in list(buffers.keys()):
            try:
                await asyncio.shield(
                    self._flush_writer_table(
                        table=table,
                        buffers=buffers,
                        buffer_rows=buffer_rows,
                        result=result,
                        result_lock=result_lock,
                    )
                )
            except Exception as e:
                logger.exception("WRITER: Error during cancellation flush for %s: %s", table, e)

    async def _flush_all_writer_buffers(
        self,
        *,
        buffers: dict[str, list[FramePacket]],
        buffer_rows: dict[str, int],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        try:
            for table in list(buffers.keys()):
                await self._flush_writer_table(
                    table=table,
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
        except Exception as e:
            logger.exception("WRITER: Error during shutdown flush: %s", e)
            raise

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
        table = packet.table
        buffers[table].append(packet)
        previous_rows = buffer_rows[table]
        current_rows = previous_rows + len(packet.frame)
        buffer_rows[table] = current_rows
        chunk_rows = max(1, int(PROGRESS_LOG_CHUNK_ROWS))
        if (current_rows // chunk_rows) > (previous_rows // chunk_rows):
            logger.debug("WRITER: Buffering %s... %d / %d rows", table, current_rows, threshold)
        if current_rows >= threshold:
            await self._flush_writer_table(
                table=table,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )

    def _writer_table_context(self, packets: list[FramePacket]) -> tuple[str, str]:
        first_packet = packets[0] if packets else None
        provider = first_packet.provider if first_packet is not None else ""
        symbol = ""
        if first_packet is None:
            return provider, symbol
        context_symbol = first_packet.context.get("symbol")
        if isinstance(context_symbol, str):
            symbol = context_symbol
            return provider, symbol
        context_ticker = first_packet.context.get("ticker")
        if isinstance(context_ticker, str):
            symbol = context_ticker
        return provider, symbol

    async def _handle_writer_flush_error(
        self,
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
        self._inc("errors_total", 1)
        provider, symbol = self._writer_table_context(packets)
        try:
            status = await spool_to_dlq_and_rescue(
                table=table,
                packets=packets,
                writer=self._writer,
                config=self._config,
                result=result,
                result_lock=result_lock,
                inc=self._inc,
                log_structured=self._log_structured,
            )
        except Exception as spool_exc:
            if isinstance(spool_exc, DLQSpoolError):
                rescued_count = spool_exc.rescued
                remaining_count = spool_exc.remaining
            else:
                rescued_count = 0
                remaining_count = 0
            status = cast(
                "DLQStatus",
                {
                    "status": "spool_failed",
                    "rescued": rescued_count,
                    "remaining": remaining_count,
                    "path": None,
                    "error": spool_exc,
                },
            )
            summary = build_writer_error_summary(
                status=status,
                table=table,
                prefix=prefix,
                exc=exc,
                provider=provider,
                symbol=symbol,
            )
            async with result_lock:
                result.errors.append(summary)
            buffers[table] = []
            buffer_rows[table] = 0
            logger.exception("WRITER: Spool failed after %s for %s: %s", prefix, table, spool_exc)
            with suppress(Exception):
                reported_exc = cast("Any", spool_exc)
                reported_exc._already_reported = True
            raise
        summary = build_writer_error_summary(
            status=status,
            table=table,
            prefix=prefix,
            exc=exc,
            provider=provider,
            symbol=symbol,
        )
        async with result_lock:
            result.errors.append(summary)
        buffers[table] = []
        buffer_rows[table] = 0

    def _concat_frames_with_flex(
        self,
        *,
        frames: list[pl.DataFrame],
        table_name: str,
        schema: object,
        rechunk: bool,
    ) -> pl.DataFrame:
        try:
            return pl.concat(frames, how="vertical", rechunk=rechunk)
        except pl.exceptions.PolarsError as e:
            is_flexible = getattr(self._router, "flexible_schema", False) or (
                schema is not None and getattr(schema, "flexible_schema", False)
            )
            if not is_flexible:
                raise
            logger.warning(
                "WRITER: Schema mismatch for %s: %s. Falling back to diagonal concat",
                table_name,
                e,
            )
            return pl.concat(frames, how="diagonal")

    def _validate_unique_key(self, *, schema: object, table: str, frame: pl.DataFrame) -> None:
        schema_obj = cast("TableSchema | None", schema)
        if schema_obj is None or not schema_obj.unique_key:
            return
        for col in schema_obj.unique_key:
            if col not in frame.columns:
                raise PrimaryKeyMissingError(table=table, column=col)
            nulls = frame.get_column(col).null_count()
            if nulls > 0:
                raise PrimaryKeyNullError(table=table, column=col, null_count=nulls)

    async def _write_merged_packet(
        self,
        *,
        table: str,
        packet: FramePacket,
        stage: str,
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        t_w0 = time.monotonic()
        with self._span("write_flush", table=table, rows=len(packet.frame)):
            write_result = await self._writer.write(packet)
        t_w1 = time.monotonic()
        self._inc("writer_flushes", 1)
        self._observe("writer_flush_duration_s", float(t_w1 - t_w0))
        self._observe("writer_rows", float(write_result.rows))
        self._observe(f"writer_flush_duration_s.{table}", float(t_w1 - t_w0))
        self._observe(f"writer_rows.{table}", float(write_result.rows))
        self._inc("rows_written_total", int(write_result.rows))
        self._log_structured(
            provider=packet.provider,
            dataset=packet.table,
            symbol=None,
            stage=stage,
            duration_s=(t_w1 - t_w0),
        )
        async with result_lock:
            result.tables[write_result.table] = result.tables.get(write_result.table, 0) + write_result.rows

    async def _flush_chunked_table(
        self,
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
        first = packets[0]
        quality_df = self._concat_frames_with_flex(
            frames=[p.frame for p in packets],
            table_name=first.table,
            schema=schema,
            rechunk=False,
        )
        await self._validate_data_quality(table=table, df=quality_df, result=result, result_lock=result_lock)
        total_rows_est = sum(len(p.frame) for p in packets)
        if total_rows_est > 0:
            est_chunks = (total_rows_est + chunk_size - 1) // chunk_size
            logger.debug(
                "WRITER: Chunking flush for %s rows~=%s chunk_size=%s chunks~=%s",
                table,
                total_rows_est,
                chunk_size,
                est_chunks,
            )
            self._log_structured(
                provider=first.provider,
                dataset=first.table,
                symbol=None,
                stage=f"write_chunking_rows_{total_rows_est}_size_{chunk_size}_chunks_{est_chunks}",
            )
        i = 0
        idx = 0
        while i < len(packets):
            rows_in_chunk = 0
            current_frames: list[pl.DataFrame] = []
            start_i = i
            while i < len(packets) and rows_in_chunk < chunk_size:
                pkt = packets[i]
                current_frames.append(pkt.frame)
                rows_in_chunk += len(pkt.frame)
                i += 1
            try:
                chunk_df = self._concat_frames_with_flex(
                    frames=current_frames,
                    table_name=first.table,
                    schema=schema,
                    rechunk=False,
                )
                self._validate_unique_key(schema=schema, table=table, frame=chunk_df)
                chunk_packet = FramePacket(
                    provider=first.provider,
                    table=first.table,
                    frame=chunk_df,
                    observed_at=first.observed_at,
                    context=first.context,
                )
                await self._write_merged_packet(
                    table=table,
                    packet=chunk_packet,
                    stage=f"write_flush_chunk_{idx + 1}",
                    result=result,
                    result_lock=result_lock,
                )
                idx += 1
            except (ComputeError, ValidationError) as e:
                remaining_packets = packets[start_i : len(packets)]
                await self._handle_writer_flush_error(
                    table=table,
                    packets=remaining_packets,
                    exc=e,
                    prefix="WriterError",
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
                if isinstance(e, PrimaryKeyMissingError):
                    logger.error("WRITER: PKMissing table=%s column=%s", table, e.column)
                elif isinstance(e, PrimaryKeyNullError):
                    logger.error("WRITER: PKNull table=%s column=%s nulls=%s", table, e.column, e.null_count)
                else:
                    logger.error("WRITER: Error writing chunk for %s: %s", table, e)
                return
            except Exception as e:
                if isinstance(e, DLQSpoolError) or getattr(e, "_already_reported", False):
                    raise
                remaining_packets = packets[start_i : len(packets)]
                is_duckdb_error = _duckdb is not None and isinstance(e, _duckdb.Error)
                prefix = "DuckDBError" if is_duckdb_error else "UnexpectedWriterError"
                await self._handle_writer_flush_error(
                    table=table,
                    packets=remaining_packets,
                    exc=e,
                    prefix=prefix,
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
                if prefix == "DuckDBError":
                    logger.exception("WRITER: DuckDB error for %s: %s", table, e)
                else:
                    logger.exception("WRITER: Unexpected error writing chunk for %s: %s", table, e)
                return

    async def _flush_legacy_table(
        self,
        *,
        table: str,
        packets: list[FramePacket],
        schema: object,
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        first = packets[0]
        merged_frame = self._concat_frames_with_flex(
            frames=[p.frame for p in packets],
            table_name=first.table,
            schema=schema,
            rechunk=True,
        )
        self._validate_unique_key(schema=schema, table=table, frame=merged_frame)
        merged_packet = FramePacket(
            provider=first.provider,
            table=first.table,
            frame=merged_frame,
            observed_at=first.observed_at,
            context=first.context,
        )
        logger.debug("WRITER: Flushing %s packets (%s rows) for %s", len(packets), len(merged_frame), table)
        await self._validate_data_quality(table=table, df=merged_frame, result=result, result_lock=result_lock)
        await self._write_merged_packet(
            table=table,
            packet=merged_packet,
            stage="write_flush",
            result=result,
            result_lock=result_lock,
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
        packets = buffers.get(table, [])
        if not packets:
            return
        first = packets[0]
        schema = get_table_schema(first.table)
        chunk_size = getattr(self._config, "writer_chunk_rows", None)
        try:
            if isinstance(chunk_size, int) and chunk_size > 0:
                await self._flush_chunked_table(
                    table=table,
                    packets=packets,
                    schema=schema,
                    chunk_size=chunk_size,
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
            else:
                await self._flush_legacy_table(
                    table=table,
                    packets=packets,
                    schema=schema,
                    result=result,
                    result_lock=result_lock,
                )
            buffers[table] = []
            buffer_rows[table] = 0
        except (ComputeError, ValidationError) as e:
            await self._handle_writer_flush_error(
                table=table,
                packets=packets,
                exc=e,
                prefix="WriterError",
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
            if isinstance(e, PrimaryKeyMissingError):
                logger.error("WRITER: PKMissing table=%s column=%s", table, e.column)
            elif isinstance(e, PrimaryKeyNullError):
                logger.error("WRITER: PKNull table=%s column=%s nulls=%s", table, e.column, e.null_count)
            else:
                logger.error("WRITER: Error writing batch for %s: %s", table, e)
        except Exception as e:
            if isinstance(e, DLQSpoolError) or getattr(e, "_already_reported", False):
                raise
            prefix = "DuckDBError" if _duckdb is not None and isinstance(e, _duckdb.Error) else "UnexpectedWriterError"
            await self._handle_writer_flush_error(
                table=table,
                packets=packets,
                exc=e,
                prefix=prefix,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
            if prefix == "DuckDBError":
                logger.exception("WRITER: DuckDB error for %s: %s", table, e)
            else:
                logger.exception("WRITER: Unexpected error writing batch for %s: %s", table, e)
                return

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
        # Honor per-request idempotency: non-idempotent requests should not retry.
        retry_controller = create_retry_controller(self._config.retry, idempotent=job.spec.idempotent)

        async for attempt in retry_controller:
            with attempt:
                state = getattr(attempt, "retry_state", None)
                att_no = getattr(state, "attempt_number", None) if state is not None else None
                async with self.controller.throttle():
                    t0 = time.monotonic()
                    self._log_structured(
                        provider=job.provider,
                        dataset=job.dataset,
                        symbol=job.symbol,
                        stage="http_start",
                        attempt=att_no,
                    )
                    try:
                        resp = await self._http.fetch(job.spec)
                        rf = getattr(self.controller, "record_feedback", None)
                        if callable(rf):
                            rf(status_code=200, retried=bool(att_no and att_no > 1))
                    except Exception as e:
                        reason = "error"
                        if isinstance(e, httpx.HTTPStatusError):
                            resp0 = getattr(e, "response", None)
                            sc = getattr(resp0, "status_code", None)
                            reason = f"http_status_{sc}"
                            rf = getattr(self.controller, "record_feedback", None)
                            if callable(rf):
                                rf(status_code=sc, retried=True)
                        elif isinstance(e, httpx.TransportError):
                            reason = "transport_error"
                            rf = getattr(self.controller, "record_feedback", None)
                            if callable(rf):
                                rf(status_code=None, retried=True)
                        else:
                            reason = type(e).__name__
                        self._log_structured(
                            provider=job.provider,
                            dataset=job.dataset,
                            symbol=job.symbol,
                            stage=f"http_retry_reason:{reason}",
                            attempt=att_no,
                        )
                        raise
                t1 = time.monotonic()
                dur = t1 - t0
                self._observe("http_duration_s", dur)
                self._log_structured(
                    provider=job.provider,
                    dataset=job.dataset,
                    symbol=job.symbol,
                    stage="http_end",
                    attempt=att_no,
                    duration_s=dur,
                )
                return resp
        # Unreachable at runtime with tenacity.AsyncRetrying(reraise=True):
        # the last exception is re-raised inside the loop when attempts are exhausted.
        # Kept for static analysis clarity and as a defensive fallback.
        raise FetchError("Fetch failed after all retry attempts")
