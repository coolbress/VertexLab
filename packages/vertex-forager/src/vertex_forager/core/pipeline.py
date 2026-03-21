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
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from functools import partial
import inspect
import itertools
import logging
import os
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
from vertex_forager.core.retry import create_retry_controller
from vertex_forager.schema.registry import get_table_schema
from vertex_forager.utils import cleanup_dlq_tmp, get_cache_dir, sanitize_field

if TYPE_CHECKING:
    from vertex_forager.core.contracts import IMapper, IRouter, IWriter
    from vertex_forager.core.controller import FlowController
    from vertex_forager.core.http import HttpExecutor
    from vertex_forager.core.types import DLQStatus

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

    async def run(
        self,
        *,
        dataset: str,
        symbols: Symbols | None,
        on_progress: Callable[..., Any] | None = None,
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
            **kwargs: Additional arguments passed to the router's generate_jobs method.

        Returns:
            RunResult: Summary of the run including metrics and errors.

        Raises:
            RuntimeError: Orchestration-level failures (e.g., early writer shutdown).

        Notes:
            - Exceptions raised during fetch/parse/write (e.g., httpx.RequestError,
              httpx.HTTPStatusError, ValidationError, PrimaryKeyMissingError,
              PrimaryKeyNullError) are captured and appended to `RunResult.errors`
              and are not re-raised by default.
            - Callers should inspect `RunResult.errors` for per-task failures and
              only expect orchestration-level issues to raise.
        """
        # PriorityQueue to prioritize pagination (next jobs) over new jobs
        # Tuple structure: (priority, order, job)
        # Priority: 0=NextJob, 10=NewJob, 999=Sentinel
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue(
            maxsize=self._config.queue_max
        )
        pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue(maxsize=self._config.queue_max)
        # Periodic cleanup of stale DLQ temp files
        if getattr(self._config, "dlq_tmp_periodic_cleanup", False):
            try:
                cleanup_dlq_tmp(get_cache_dir() / "dlq", int(getattr(self._config, "dlq_tmp_retention_s", 86400)))
            except Exception as _e_clean:
                logger.warning("PIPELINE: DLQ periodic cleanup failed: %s", _e_clean)

        if self._metrics_enabled:
            self._counters = {}
            self._hists = {}
            self._summary = {}
            self._counters["pipeline_runs"] = 1
        t_run0 = time.monotonic()

        result = RunResult(provider=self._router.provider)
        result_lock = asyncio.Lock()

        writer_tasks = [
            asyncio.create_task(
                self._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock),
                name="vertex-forager:writer:0",
            )
        ]

        fetch_tasks = [
            asyncio.create_task(
                self._fetch_worker(
                    i,
                    req_q=req_q,
                    pkt_q=pkt_q,
                    result=result,
                    result_lock=result_lock,
                    on_progress=on_progress,
                ),
                name=f"vertex-forager:fetch:{i}",
            )
            for i in range(self.controller.concurrency_limit)
        ]

        producer_task = asyncio.create_task(
            self._producer(req_q=req_q, dataset=dataset, symbols=symbols, **kwargs),
            name="vertex-forager:producer",
        )

        # Register tasks for stop()
        self._active_tasks = [producer_task, *fetch_tasks, *writer_tasks]
        # Expose queues and worker references for unified shutdown in stop()
        self._req_q = req_q
        self._pkt_q = pkt_q
        self._writer_tasks = writer_tasks
        # Initialize global fairness state
        self._fair_lock = asyncio.Lock()
        self._fair_last_symbol = None
        self._fair_burst_count = 0
        # Reset writer flush guards for a new run
        self._writer_flush_attempted = False
        self._writer_flushed = False

        try:
            # Create a monitor for writer tasks to detect early failures
            writer_monitor = asyncio.gather(*writer_tasks)
            writer_monitor = cast("asyncio.Future[Any]", writer_monitor)

            async def _pipeline_orchestration() -> None:
                """Orchestrate the producer-fetcher-join sequence."""
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
                    # Queue length snapshot after producer completes
                    if self._metrics_enabled:
                        with suppress(Exception):
                            self._summary["req_q_len_after_producer"] = float(req_q.qsize())
                            self._summary["pkt_q_len_after_producer"] = float(pkt_q.qsize())
                await req_q.join()
                logger.info("PIPELINE: Request queue joined, sending sentinel signals...")
                if self._metrics_enabled:
                    with suppress(Exception):
                        self._summary["req_q_len_after_req_join"] = float(req_q.qsize())
                for _ in range(self.controller.concurrency_limit):
                    # Sentinel with lowest priority (highest number)
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

            # Run orchestration concurrently with writer monitor
            orchestrator = asyncio.create_task(_pipeline_orchestration(), name="vertex-forager:orchestrator")
            orchestrator = cast("asyncio.Task[Any]", orchestrator)
            self._active_tasks.append(orchestrator)

            futures: set[asyncio.Future[Any]] = {writer_monitor, cast("asyncio.Future[Any]", orchestrator)}
            done, _pending = await asyncio.wait(
                futures,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Check if writers failed early
            if writer_monitor in done and orchestrator not in done:
                # Writer failed while pipeline was running
                exc = writer_monitor.exception()
                if exc:
                    raise exc
                # If it finished without exception but orchestrator is still running,
                # it means it exited prematurely (e.g. consumed None that wasn't sent?)
                # This shouldn't happen with correct logic.
                raise RuntimeError("Writer exited prematurely")

            # Ensure orchestrator completes (if it wasn't the one that finished)
            await orchestrator

            # Wait for writers to complete (graceful shutdown)
            logger.debug("PIPELINE: Waiting for writer tasks to complete...")
            await writer_monitor

            # Flush any buffered data in the writer
            logger.debug("PIPELINE: Flushing writer buffer...")
            await self._try_flush_once()

            logger.info(f"PIPELINE: Run completed. Total errors: {len(result.errors)}")
            if self._metrics_enabled:
                # Merge counters from mapper/writer if they expose them
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
                result.metrics_counters = dict(self._counters)
                result.metrics_histograms = {k: list(v) for k, v in self._hists.items()}
                self._summary = self._compute_summary()
                result.metrics_summary = dict(self._summary)
                sink = self._metrics_sink
                if sink is not None:
                    with suppress(Exception):
                        sink.summary(dict(self._summary))
                if self._structured_logs:
                    dur_run = time.monotonic() - t_run0
                    msg_s = (
                        f"OBS provider={sanitize_field(self._router.provider)} "
                        f"dataset={sanitize_field(dataset)} symbol=* stage=pipeline_summary attempt=0 "
                        f"duration={dur_run:.3f}s " + " ".join(f"{k}={v:.3f}" for k, v in sorted(self._summary.items()))
                    )
                    if self._log_verbose:
                        logger.info(msg_s)
                    else:
                        logger.debug(msg_s)
            return result
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully stop the pipeline.

        Cancels all running tasks (producer, fetchers, writers) and awaits their
        cleanup. This method is idempotent and safe to call multiple times.

        Notes:
            - Internally, this method calls `asyncio.gather(*self._active_tasks, return_exceptions=True)`,
              which collects `asyncio.CancelledError` as a returned exception rather than raising it.
            - `asyncio.CancelledError` would only propagate if the `stop` coroutine itself is externally
              cancelled by the caller while awaiting completion.

        Raises:
            asyncio.CancelledError: Only if this `stop` coroutine is externally cancelled; exceptions
            from tasks are logged and suppressed via `return_exceptions=True`.
        """
        if not self._active_tasks:
            return

        logger.debug("PIPELINE: Stopping pipeline...")
        writer_tasks = getattr(self, "_writer_tasks", None)
        writer_set: set[asyncio.Task[Any]] = (
            set(cast("list[asyncio.Task[Any]]", writer_tasks)) if isinstance(writer_tasks, list) else set()
        )
        other_tasks: list[asyncio.Task[Any]] = [
            t for t in cast("list[asyncio.Task[Any]]", self._active_tasks) if t not in writer_set
        ]
        for task in other_tasks:
            if not task.done():
                task.cancel()
        if other_tasks:
            await asyncio.gather(*other_tasks, return_exceptions=True)
        # Best-effort signal queues to unblock waiting workers, without blocking stop()
        try:
            req_q = getattr(self, "_req_q", None)
            fetch_n = int(getattr(self.controller, "concurrency_limit", 0) or 0)
            if req_q is not None and fetch_n > 0:
                for _ in range(fetch_n):
                    try:
                        req_q.put_nowait((self.PRIORITY_SENTINEL, 0, None))
                    except Exception as e:
                        # Queue may be full or already closed; ignore to avoid deadlock
                        logger.debug("PIPELINE: Skipping req_q sentinel (%s)", e)
                        break
        except Exception as e:
            logger.debug("PIPELINE: Failed to enqueue request sentinels during stop: %s", e, exc_info=True)
        try:
            pkt_q = getattr(self, "_pkt_q", None)
            active_writers = 0
            if isinstance(writer_tasks, list):
                try:
                    active_writers = sum(1 for t in writer_tasks if isinstance(t, asyncio.Task) and not t.done())
                except Exception:
                    active_writers = len(writer_tasks)
            sentinel_put_tasks: list[asyncio.Task[Any]] = []
            if pkt_q is not None and active_writers > 0:
                remaining = active_writers
                for _ in range(active_writers):
                    try:
                        pkt_q.put_nowait(None)
                        remaining -= 1
                    except Exception as e:
                        logger.debug("PIPELINE: pkt_q full when enqueuing sentinel (%s), scheduling async puts", e)
                        break
                if remaining > 0:
                    for _ in range(remaining):
                        try:
                            t = asyncio.create_task(pkt_q.put(None))
                            sentinel_put_tasks.append(t)
                        except Exception as e:
                            logger.debug("PIPELINE: Failed to schedule pkt_q.put(None) task: %s", e)
        except Exception as e:
            logger.debug("PIPELINE: Failed to enqueue packet sentinels during stop: %s", e, exc_info=True)
        # First, time-limit only the sentinel put tasks to avoid blocking the shutdown
        if sentinel_put_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*sentinel_put_tasks, return_exceptions=True), timeout=10.0
                )
            except asyncio.TimeoutError:
                for t in sentinel_put_tasks:
                    with suppress(Exception):
                        t.cancel()
                logger.debug("PIPELINE: Timeout awaiting pkt_q sentinel put tasks; cancelled pending puts")
                await self._try_flush_once()
                # If writers could still be blocked on pkt_q.get(), cancel them to avoid hang
                if writer_set:
                    for w in list(writer_set):
                        with suppress(Exception):
                            w.cancel()
                    with suppress(Exception):
                        await asyncio.gather(*writer_set, return_exceptions=True)
                    writer_set = set()
        # Then, always await writers without a timeout to let them drain gracefully
        try:
            if writer_set:
                await asyncio.gather(*writer_set, return_exceptions=True)
        except Exception:
            logger.debug("PIPELINE: Awaiting writer tasks raised but suppressed", exc_info=True)
        self._active_tasks.clear()
        # Ensure writer flush on shutdown for DLQ guarantees
        try:
            await self._try_flush_once()
        except Exception:
            logger.debug("PIPELINE: Writer flush on stop raised but suppressed", exc_info=True)
        try:
            self._parse_executor.shutdown(wait=False)
        except (RuntimeError, ValueError) as e:
            logger.exception(f"PIPELINE: Parse executor shutdown failed: {e}")
        except Exception:
            logger.exception("PIPELINE: Unexpected error during parse executor shutdown")
        logger.debug("PIPELINE: Pipeline stopped.")

    async def _producer(
        self,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        dataset: str,
        symbols: Symbols | None,
        **kwargs: object,
    ) -> None:
        """Generate fetch jobs and push them to the request queue.

        Iterates through the provider's `generate_jobs` generator. Once all jobs are
        enqueued, fetch workers may enqueue additional jobs (e.g., pagination).
        """
        counter = itertools.count()
        job_count = 0
        logger.debug(
            f"PRODUCER: Starting job generation for dataset={dataset}, symbols={len(symbols) if symbols else 'all'}"
        )

        async for job in self._router.generate_jobs(dataset=dataset, symbols=symbols, **kwargs):
            await req_q.put((self.PRIORITY_NEW_JOB, next(counter), job))
            job_count += 1
            self._inc("jobs_generated", 1)
            if job_count % 100 == 0:
                logger.debug(f"PRODUCER: Generated {job_count} jobs so far...")

        logger.debug(f"PRODUCER: Completed job generation. Total jobs: {job_count}")

    async def _try_flush_once(self) -> None:
        if not getattr(self, "_writer_flush_attempted", False) and not getattr(self, "_writer_flushed", False):
            self._writer_flush_attempted = True
            try:
                await self._writer.flush()
                self._writer_flushed = True
            except Exception:
                logger.debug("PIPELINE: Writer flush attempt suppressed", exc_info=True)
            finally:
                self._writer_flush_attempted = True

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
        if self._fair_lock is None:
            self._fair_lock = asyncio.Lock()
            self._fair_last_symbol = None
            self._fair_burst_count = 0
        demote_jobs: list[FetchJob] = []
        already_done = False
        async with self._fair_lock:
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
                while True:
                    try:
                        p2, _, cand = req_q.get_nowait()
                    except asyncio.QueueEmpty:
                        # Need to wait for another candidate; loop back and get() again under the lock
                        break
                    if p2 == self.PRIORITY_PAGINATION and cand is not None and cand.symbol == self._fair_last_symbol:
                        demote_jobs.append(cand)
                        continue
                    # Found a different candidate; update fairness baseline accordingly and return it
                    if p2 == self.PRIORITY_PAGINATION and cand is not None:
                        self._fair_last_symbol = cand.symbol
                        self._fair_burst_count = 1
                    else:
                        self._fair_last_symbol = cand.symbol if cand is not None else None
                        self._fair_burst_count = 0
                    return p2, cand, demote_jobs, already_done
                # If we broke due to empty queue, continue the outer loop to await another item under lock

    async def _fetch_worker(
        self,
        worker_id: int,
        *,
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
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

        # Optimization: Pre-calculate progress handler to avoid repeated inspection
        async def noop_handler(
            job: FetchJob,
            payload: bytes | None,
            exc: Exception | None,
            parse_result: ParseResult | None,
        ) -> None:
            pass

        handler = noop_handler
        if on_progress:
            try:
                sig = inspect.signature(on_progress)
                wants_parse_result = "parse_result" in sig.parameters
                is_async = inspect.iscoroutinefunction(on_progress)

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
                        logger.error(f"Error in on_progress callback: {e}")
                        # Don't re-raise to keep worker alive

                handler = _progress_wrapper
            except Exception as e:
                logger.warning(f"Failed to bind on_progress handler: {e}")

        job_count = 0
        burst_cap = getattr(self._config, "pagination_max_burst", None)
        while True:
            priority, job, demote_jobs, already_done = await self._pop_next_job_respecting_fairness(
                req_q=req_q, burst_cap=burst_cap
            )
            # Apply demotions outside the lock
            for dj in demote_jobs:
                await req_q.put((self.PRIORITY_NEW_JOB, time.monotonic_ns(), dj))
                req_q.task_done()
            if job is None:
                logger.debug(
                    f"[Worker-{worker_id}] Received sentinel, shutting down. Total jobs processed: {job_count}"
                )
                # already_done indicates we consumed the sentinel and called task_done
                if not already_done:
                    req_q.task_done()
                return
            job_count += 1
            self._inc("jobs_processed", 1)
            if job_count % 100 == 0:
                logger.debug(f"[Worker-{worker_id}] Processed {job_count} jobs so far...")

            payload: bytes | None = None
            worker_exc: Exception | None = None
            parse_result: ParseResult | None = None
            try:
                # Log Fetch Start
                logger.debug(f"[Worker-{worker_id}] Processing job: {job.symbol} (priority: {priority})")
                self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="fetch_start")

                t_fetch_start = time.monotonic()
                with self._span("fetch", provider=job.provider, dataset=job.dataset, symbol=str(job.symbol)):
                    payload = await self._fetch_with_retry(job)
                t_fetch_end = time.monotonic()
                fetch_latency = t_fetch_end - t_fetch_start
                self._observe("fetch_duration_s", fetch_latency)
                self._log_structured(
                    provider=job.provider,
                    dataset=job.dataset,
                    symbol=job.symbol,
                    stage="fetch_end",
                    duration_s=fetch_latency,
                )

                t1 = time.monotonic()
                logger.debug(
                    f"[Worker-{worker_id}] Fetched {job.symbol} "
                    f"({len(payload) if payload else 0} bytes) "
                    f"in {fetch_latency:.3f}s"
                )
                self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="parse_start")

                # Offload CPU-bound parsing to a dedicated thread pool
                loop = asyncio.get_running_loop()
                with self._span(
                    "parse",
                    provider=job.provider,
                    dataset=job.dataset,
                    symbol=str(job.symbol),
                ):
                    parse_result = await loop.run_in_executor(
                        self._parse_executor,
                        partial(self._router.parse, job=job, payload=payload),
                    )
                t2 = time.monotonic()
                parse_latency = t2 - t1
                self._observe("parse_duration_s", parse_latency)
                logger.debug(
                    f"[Worker-{worker_id}] Parsed {job.symbol} "
                    f"in {parse_latency:.3f}s. "
                    f"Packets: {len(parse_result.packets)}, "
                    f"Next Jobs: {len(parse_result.next_jobs)}"
                )
                self._log_structured(
                    provider=job.provider,
                    dataset=job.dataset,
                    symbol=job.symbol,
                    stage="parse_end",
                    duration_s=parse_latency,
                )

                for packet in parse_result.packets:
                    # Normalize packet schema (enforce types, fill missing cols)
                    loop = asyncio.get_running_loop()
                    normalized_packet = await loop.run_in_executor(
                        self._parse_executor, partial(self._mapper.normalize, packet=packet)
                    )
                    await pkt_q.put(normalized_packet)
                    self._inc("packets_emitted", 1)

                if parse_result.next_jobs:
                    logger.debug(
                        f"[Worker-{worker_id}] Adding {len(parse_result.next_jobs)} pagination jobs for {job.symbol}"
                    )
                    for next_job in parse_result.next_jobs:
                        await req_q.put((self.PRIORITY_PAGINATION, time.monotonic_ns(), next_job))

            except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as exc:
                worker_exc = exc
                async with result_lock:
                    result.errors.append(f"{job.provider}:{job.dataset}:{job.symbol}:{exc}")
                logger.error(f"[Worker-{worker_id}] Error processing {job.symbol}: {exc}")
                self._inc("errors_total", 1)
                self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="error")
            except FetchError as exc:
                worker_exc = exc
                async with result_lock:
                    result.errors.append(f"{job.provider}:{job.dataset}:{job.symbol}:{exc}")
                logger.error(f"[Worker-{worker_id}] Fetch exhausted for {job.symbol}: {exc}")
                self._inc("errors_total", 1)
                self._log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="error_fetch")
            except Exception as exc:
                worker_exc = exc
                async with result_lock:
                    result.errors.append(f"Unexpected:{job.provider}:{job.dataset}:{job.symbol}:{exc}")
                logger.exception(f"[Worker-{worker_id}] Unexpected error processing {job.symbol}: {exc}")
                self._inc("errors_total", 1)
                self._log_structured(
                    provider=job.provider,
                    dataset=job.dataset,
                    symbol=job.symbol,
                    stage="error_unexpected",
                )
            finally:
                req_q.task_done()
                try:
                    await handler(job, payload, worker_exc, parse_result)
                except Exception as e:
                    # Swallowing exception from callback to prevent worker crash
                    logger.error(
                        f"[Worker-{worker_id}] Error in result handler for "
                        f"{job.provider}:{job.dataset}:{job.symbol}: {e}"
                    )

    async def _writer_worker(
        self,
        *,
        pkt_q: asyncio.Queue[FramePacket | None],
        result: RunResult,
        result_lock: asyncio.Lock,
    ) -> None:
        """Consume packets and write to storage with adaptive bulk writing.

        Optimized Strategy:
        1. Buffers packets in memory until `flush_threshold_rows` is reached.
        2. Merges small packets into larger chunks using Polars.
        3. Writes large chunks to DuckDB in a single transaction.
        4. Minimizes IOPS and Lock contention.
        """
        # Buffer: table_name -> list of packets
        buffers: dict[str, list[FramePacket]] = defaultdict(list)
        buffer_rows: dict[str, int] = defaultdict(int)

        # Use config value
        threshold = self._flush_threshold
        logger.debug(f"WRITER: Adaptive bulk writing enabled. Threshold={threshold} rows")

        async def _update_dlq_counts(*, table: str, rescued: int, remaining: int) -> None:
            async with result_lock:
                entry = result.dlq_counts.get(table) or {"rescued": 0, "remaining": 0}
                if rescued:
                    entry["rescued"] = entry.get("rescued", 0) + int(rescued)
                if remaining:
                    entry["remaining"] = entry.get("remaining", 0) + int(remaining)
                result.dlq_counts[table] = entry

        async def _spool_to_dlq_and_rescue(table: str, packets: list[FramePacket], err: Exception) -> DLQStatus:
            if not packets:
                return {"status": "noop", "rescued": 0, "remaining": 0, "path": None, "error": None}
            first = packets[0]
            rescued = 0
            failed_packets: list[FramePacket] = []
            max_consecutive_failures = 3
            consecutive_failures = 0

            def _validate_pk_for_rescue(pkt: FramePacket, schema_obj: Any) -> tuple[bool, str | None]:
                if schema_obj and getattr(schema_obj, "unique_key", None):
                    for col in schema_obj.unique_key:
                        if col not in pkt.frame.columns:
                            return False, "missing"
                        nulls = pkt.frame.get_column(col).null_count()
                        if nulls > 0:
                            return False, "null"
                return True, None

            for pkt in packets:
                try:
                    schema = get_table_schema(pkt.table)
                    is_valid, reason = _validate_pk_for_rescue(pkt, schema)
                    if not is_valid:
                        logger.debug(
                            "DLQ rescue skipped: PK validation failed reason=%s table=%s provider=%s rows=%d",
                            reason,
                            table,
                            pkt.provider,
                            len(pkt.frame),
                        )
                        failed_packets.append(pkt)
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            processed = rescued + len(failed_packets)
                            if processed < len(packets):
                                failed_packets.extend(packets[processed:])
                            break
                        continue

                    wr = await self._writer.write(pkt)
                    async with result_lock:
                        result.tables[wr.table] = result.tables.get(wr.table, 0) + wr.rows
                    rescued += 1
                    consecutive_failures = 0
                except Exception as e2:
                    failed_packets.append(pkt)
                    logger.debug(
                        "DLQ rescue write failed table=%s provider=%s rows=%d error=%s",
                        table,
                        pkt.provider,
                        len(pkt.frame),
                        e2,
                    )
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        processed = rescued + len(failed_packets)
                        if processed < len(packets):
                            # Append remaining unprocessed packets to fail list to ensure they are spooled
                            failed_packets.extend(packets[processed:])
                        break
            remaining = len(failed_packets)
            if remaining > 0 and not bool(getattr(self._config, "dlq_enabled", True)):
                self._log_structured(provider=first.provider, dataset=table, symbol=None, stage="dlq_disabled")
                self._log_structured(
                    provider=first.provider,
                    dataset=table,
                    symbol=None,
                    stage=f"dlq_rescued_{rescued}",
                )
                self._log_structured(
                    provider=first.provider,
                    dataset=table,
                    symbol=None,
                    stage=f"dlq_remaining_{remaining}",
                )
                self._inc("dlq_disabled_total", 1)
                self._inc(f"dlq_disabled.{table}", 1)
                if rescued:
                    self._inc("dlq_rescued_total", int(rescued))
                    self._inc(f"dlq_rescued.{table}", int(rescued))
                if remaining:
                    self._inc("dlq_remaining_total", int(remaining))
                    self._inc(f"dlq_remaining.{table}", int(remaining))
                await _update_dlq_counts(table=table, rescued=int(rescued), remaining=int(remaining))
                return {"status": "disabled", "rescued": rescued, "remaining": remaining, "path": None, "error": None}
            if remaining > 0:
                try:
                    tmp_path = None
                    frames = [p.frame for p in failed_packets]
                    try:
                        merged = pl.concat(frames, how="vertical", rechunk=True)
                    except pl.exceptions.PolarsError:
                        merged = pl.concat(frames, how="diagonal")
                    dlq_dir = get_cache_dir() / "dlq" / table
                    dlq_dir.mkdir(parents=True, exist_ok=True)
                    ts_ns = time.time_ns()
                    fpath = dlq_dir / f"batch_{ts_ns}.ipc"
                    tmp_path = fpath.parent / (f"{fpath.name}.tmp")
                    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                    with os.fdopen(fd, "wb") as fh:
                        merged.write_ipc(fh)
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.replace(tmp_path, fpath)
                    with suppress(Exception):
                        dir_fd = os.open(str(dlq_dir), os.O_RDONLY)
                        try:
                            os.fsync(dir_fd)
                        finally:
                            os.close(dir_fd)
                    # Emit per-count structured logs even on early return
                    self._log_structured(
                        provider=first.provider,
                        dataset=table,
                        symbol=None,
                        stage=f"dlq_rescued_{rescued}",
                    )
                    self._log_structured(
                        provider=first.provider,
                        dataset=table,
                        symbol=None,
                        stage=f"dlq_remaining_{remaining}",
                    )
                    # Metrics: DLQ spooled and counts
                    self._inc("dlq_spooled_files_total", 1)
                    self._inc(f"dlq_spooled_files.{table}", 1)
                    if rescued:
                        self._inc("dlq_rescued_total", int(rescued))
                        self._inc(f"dlq_rescued.{table}", int(rescued))
                    if remaining:
                        self._inc("dlq_remaining_total", int(remaining))
                        self._inc(f"dlq_remaining.{table}", int(remaining))
                    await _update_dlq_counts(table=table, rescued=int(rescued), remaining=int(remaining))
                    return {
                        "status": "spooled",
                        "rescued": rescued,
                        "remaining": remaining,
                        "path": str(fpath),
                        "error": None,
                    }
                except Exception as exc:
                    if getattr(self._config, "dlq_tmp_cleanup_on_error", False):
                        try:
                            if tmp_path is not None and tmp_path.exists():
                                tmp_path.unlink()
                                with suppress(Exception):
                                    dir_fd = os.open(str(tmp_path.parent), os.O_RDONLY)
                                    try:
                                        os.fsync(dir_fd)
                                    finally:
                                        os.close(dir_fd)
                        except Exception as _e_del:
                            logger.warning("DLQ tmp on-error cleanup failed for %s: %s", tmp_path, _e_del)
                    async with result_lock:
                        pending = result.dlq_pending.get(table, [])
                        pending.extend(failed_packets)
                        result.dlq_pending[table] = pending
                    rescued_count = rescued
                    remaining_count = len(failed_packets)
                    self._log_structured(
                        provider=first.provider,
                        dataset=table,
                        symbol=None,
                        stage=f"dlq_rescued_{rescued_count}",
                    )
                    self._log_structured(
                        provider=first.provider,
                        dataset=table,
                        symbol=None,
                        stage=f"dlq_remaining_{remaining_count}",
                    )
                    # Metrics: DLQ spool failed
                    self._inc("dlq_spool_failed_total", 1)
                    if rescued_count:
                        self._inc("dlq_rescued_total", int(rescued_count))
                        self._inc(f"dlq_rescued.{table}", int(rescued_count))
                    if remaining_count:
                        self._inc("dlq_remaining_total", int(remaining_count))
                        self._inc(f"dlq_remaining.{table}", int(remaining_count))
                    await _update_dlq_counts(table=table, rescued=int(rescued_count), remaining=int(remaining_count))
                    raise DLQSpoolError(rescued=rescued_count, remaining=remaining_count, original=exc) from exc
            self._log_structured(provider=first.provider, dataset=table, symbol=None, stage=f"dlq_rescued_{rescued}")
            self._log_structured(
                provider=first.provider,
                dataset=table,
                symbol=None,
                stage=f"dlq_remaining_{remaining}",
            )
            # At this point, any remaining > 0 would have been handled in the spooling block (return/raise).
            # Return rescued_only for clarity.
            # Metrics: rescued only
            if rescued:
                self._inc("dlq_rescued_total", int(rescued))
                self._inc(f"dlq_rescued.{table}", int(rescued))
            await _update_dlq_counts(table=table, rescued=int(rescued), remaining=0)
            return {"status": "rescued_only", "rescued": rescued, "remaining": 0, "path": None, "error": None}

        def _build_writer_error_summary(*, status: DLQStatus, table: str, prefix: str, exc: Exception) -> str:
            match status["status"]:
                case "spooled":
                    path = status["path"]
                    summary = (
                        f"{prefix}:{table}:{exc} "
                        f"(DLQ=spooled; rescued={status['rescued']}; remaining={status['remaining']}; path={path})"
                    )
                case "rescued_only":
                    summary = (
                        f"{prefix}:{table}:{exc} "
                        f"(DLQ=rescued_only; rescued={status['rescued']}; remaining={status['remaining']})"
                    )
                case "noop":
                    summary = (
                        f"{prefix}:{table}:{exc} "
                        f"(DLQ=noop; rescued={status['rescued']}; remaining={status['remaining']})"
                    )
                case "spool_failed":
                    # Built and appended before re-raising the spool exception to the outer handler.
                    summary = (
                        f"{prefix}:{table}:{exc} "
                        f"(DLQ=spool_failed; rescued={status['rescued']}; remaining={status['remaining']})"
                    )
                case "disabled":
                    summary = (
                        f"{prefix}:{table}:{exc} "
                        f"(DLQ=disabled; rescued={status['rescued']}; remaining={status['remaining']})"
                    )
                case _:
                    # Fallback if a new status is introduced without updating this function
                    summary = f"{prefix}:{table}:{exc} (DLQ={status['status']})"
            return summary

        async def _handle_flush_error(
            *,
            table: str,
            packets: list[FramePacket],
            exc: Exception,
            prefix: str,
        ) -> None:
            self._inc("errors_total", 1)
            try:
                status = await _spool_to_dlq_and_rescue(table, packets, exc)
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
                summary = _build_writer_error_summary(status=status, table=table, prefix=prefix, exc=exc)
                async with result_lock:
                    result.errors.append(summary)
                buffers[table] = []
                buffer_rows[table] = 0
                logger.exception(f"WRITER: Spool failed after {prefix} for {table}: {spool_exc}")
                with suppress(Exception):
                    _attr = "_already_reported"
                    setattr(spool_exc, _attr, True)
                raise
            summary = _build_writer_error_summary(status=status, table=table, prefix=prefix, exc=exc)
            async with result_lock:
                result.errors.append(summary)
            buffers[table] = []
            buffer_rows[table] = 0

        async def flush(table: str) -> None:
            """Flush the buffer for a specific table.

            Note: This helper does not call task_done(). Queue consumption is handled
            by the calling loop (main loop or shutdown sequence).
            """
            packets = buffers.get(table, [])
            if not packets:
                return

            try:
                first = packets[0]
                schema = get_table_schema(first.table)

                chunk_size = getattr(self._config, "writer_chunk_rows", None)
                if isinstance(chunk_size, int) and chunk_size > 0:
                    # Streaming chunked merge/write to minimize memory peak
                    total_rows_est = 0
                    try:
                        total_rows_est = sum(len(p.frame) for p in packets)
                    except Exception:
                        total_rows_est = 0
                    if total_rows_est > 0:
                        est_chunks = (total_rows_est + chunk_size - 1) // chunk_size
                        logger.debug(
                            f"WRITER: Chunking flush for {table} "
                            f"rows~={total_rows_est} "
                            f"chunk_size={chunk_size} "
                            f"chunks~={est_chunks}"
                        )
                        self._log_structured(
                            provider=first.provider,
                            dataset=first.table,
                            symbol=None,
                            stage=f"write_chunking_rows_{total_rows_est}_size_{chunk_size}_chunks_{est_chunks}",
                        )
                    # Accumulate packets into size-bounded chunks
                    idx = 0
                    i = 0
                    n = len(packets)
                    while i < n:
                        rows_in_chunk = 0
                        current_frames: list[pl.DataFrame] = []
                        current_packets: list[FramePacket] = []
                        start_i = i
                        while i < n and rows_in_chunk < chunk_size:
                            pkt = packets[i]
                            current_packets.append(pkt)
                            current_frames.append(pkt.frame)
                            rows_in_chunk += len(pkt.frame)
                            i += 1
                        # Concatenate only the current chunk
                        try:
                            try:
                                chunk_df = pl.concat(current_frames, how="vertical", rechunk=False)
                            except pl.exceptions.PolarsError as e:
                                is_flexible = getattr(self._router, "flexible_schema", False) or (
                                    schema is not None and getattr(schema, "flexible_schema", False)
                                )
                                if not is_flexible:
                                    raise
                                logger.warning(
                                    "WRITER: Schema mismatch for %s: %s. Falling back to diagonal concat",
                                    first.table,
                                    e,
                                )
                                chunk_df = pl.concat(current_frames, how="diagonal")
                            if schema and schema.unique_key:
                                for col in schema.unique_key:
                                    if col not in chunk_df.columns:
                                        raise PrimaryKeyMissingError(table=table, column=col)
                                    nulls = chunk_df.get_column(col).null_count()
                                    if nulls > 0:
                                        raise PrimaryKeyNullError(table=table, column=col, null_count=nulls)
                            chunk_packet: FramePacket = FramePacket(
                                provider=first.provider,
                                table=first.table,
                                frame=chunk_df,
                                observed_at=first.observed_at,
                                context=first.context,
                            )
                            t_w0 = time.monotonic()
                            with self._span("write_flush", table=table, rows=len(chunk_df)):
                                write_result = await self._writer.write(chunk_packet)
                            t_w1 = time.monotonic()
                            self._inc("writer_flushes", 1)
                            self._observe("writer_flush_duration_s", float(t_w1 - t_w0))
                            self._observe("writer_rows", float(write_result.rows))
                            # Per-table observations
                            self._observe(f"writer_flush_duration_s.{table}", float(t_w1 - t_w0))
                            self._observe(f"writer_rows.{table}", float(write_result.rows))
                            self._inc("rows_written_total", int(write_result.rows))
                            self._log_structured(
                                provider=first.provider,
                                dataset=first.table,
                                symbol=None,
                                stage=f"write_flush_chunk_{idx + 1}",
                                duration_s=(t_w1 - t_w0),
                            )
                            async with result_lock:
                                result.tables[write_result.table] = (
                                    result.tables.get(write_result.table, 0) + write_result.rows
                                )
                            idx += 1
                        except (ComputeError, ValidationError) as e:
                            # Only unprocessed packets (current chunk + remaining) should be forwarded
                            remaining_packets = packets[start_i:n]
                            await _handle_flush_error(
                                table=table,
                                packets=remaining_packets,
                                exc=e,
                                prefix="WriterError",
                            )
                            if isinstance(e, PrimaryKeyMissingError):
                                logger.error("WRITER: PKMissing table=%s column=%s", table, e.column)
                            elif isinstance(e, PrimaryKeyNullError):
                                logger.error(
                                    "WRITER: PKNull table=%s column=%s nulls=%s",
                                    table,
                                    e.column,
                                    e.null_count,
                                )
                            else:
                                logger.error("WRITER: Error writing chunk for %s: %s", table, e)
                            return
                        except Exception as e:
                            # Prevent duplicate DLQ handling if inner handler already raised a DLQSpoolError
                            if isinstance(e, DLQSpoolError) or getattr(e, "_already_reported", False):
                                raise
                            remaining_packets = packets[start_i:n]
                            if _duckdb is not None and isinstance(e, _duckdb.Error):
                                await _handle_flush_error(
                                    table=table,
                                    packets=remaining_packets,
                                    exc=e,
                                    prefix="DuckDBError",
                                )
                                logger.exception(f"WRITER: DuckDB error for {table}: {e}")
                            else:
                                await _handle_flush_error(
                                    table=table,
                                    packets=remaining_packets,
                                    exc=e,
                                    prefix="UnexpectedWriterError",
                                )
                                logger.exception(f"WRITER: Unexpected error writing chunk for {table}: {e}")
                            return
                else:
                    # Legacy path: merge all then write once
                    frames = [p.frame for p in packets]
                    try:
                        merged_frame = pl.concat(frames, how="vertical", rechunk=True)
                    except pl.exceptions.PolarsError as e:
                        is_flexible = getattr(self._router, "flexible_schema", False) or (
                            schema is not None and getattr(schema, "flexible_schema", False)
                        )
                        if not is_flexible:
                            raise
                        logger.warning(
                            "WRITER: Schema mismatch for %s: %s. Falling back to diagonal concat",
                            first.table,
                            e,
                        )
                        merged_frame = pl.concat(frames, how="diagonal")
                    if schema and schema.unique_key:
                        for col in schema.unique_key:
                            if col not in merged_frame.columns:
                                raise PrimaryKeyMissingError(table=table, column=col)
                            nulls = merged_frame.get_column(col).null_count()
                            if nulls > 0:
                                raise PrimaryKeyNullError(table=table, column=col, null_count=nulls)
                    merged_packet: FramePacket = FramePacket(
                        provider=first.provider,
                        table=first.table,
                        frame=merged_frame,
                        observed_at=first.observed_at,
                        context=first.context,
                    )
                    logger.debug(f"WRITER: Flushing {len(packets)} packets ({len(merged_frame)} rows) for {table}")
                    t_w0 = time.monotonic()
                    with self._span("write_flush", table=table, rows=len(merged_frame)):
                        write_result = await self._writer.write(merged_packet)
                    t_w1 = time.monotonic()
                    self._inc("writer_flushes", 1)
                    self._observe("writer_flush_duration_s", float(t_w1 - t_w0))
                    self._observe("writer_rows", float(write_result.rows))
                    self._observe(f"writer_flush_duration_s.{table}", float(t_w1 - t_w0))
                    self._observe(f"writer_rows.{table}", float(write_result.rows))
                    self._inc("rows_written_total", int(write_result.rows))
                    self._log_structured(
                        provider=merged_packet.provider,
                        dataset=merged_packet.table,
                        symbol=None,
                        stage="write_flush",
                        duration_s=(t_w1 - t_w0),
                    )
                    async with result_lock:
                        result.tables[write_result.table] = result.tables.get(write_result.table, 0) + write_result.rows

                # Clear buffer only after successful write
                buffers[table] = []
                buffer_rows[table] = 0

            except (ComputeError, ValidationError) as e:
                await _handle_flush_error(table=table, packets=packets, exc=e, prefix="WriterError")
                if isinstance(e, PrimaryKeyMissingError):
                    logger.error("WRITER: PKMissing table=%s column=%s", table, e.column)
                elif isinstance(e, PrimaryKeyNullError):
                    logger.error("WRITER: PKNull table=%s column=%s nulls=%s", table, e.column, e.null_count)
                else:
                    logger.error("WRITER: Error writing batch for %s: %s", table, e)
            except Exception as e:
                # If DLQSpoolError bubbled up or the error was already reported, propagate to avoid duplication
                if isinstance(e, DLQSpoolError) or getattr(e, "_already_reported", False):
                    raise
                # Check for DuckDB Error if available
                if _duckdb is not None and isinstance(e, _duckdb.Error):
                    await _handle_flush_error(table=table, packets=packets, exc=e, prefix="DuckDBError")
                    logger.exception(f"WRITER: DuckDB error for {table}: {e}")
                else:
                    await _handle_flush_error(table=table, packets=packets, exc=e, prefix="UnexpectedWriterError")
                    logger.exception(f"WRITER: Unexpected error writing batch for {table}: {e}")
                    # Do not re-raise to avoid outer handler adding duplicate Writer:Unexpected
                    return

        while True:
            packet = await pkt_q.get()
            try:
                if packet is None:
                    # Flush all remaining buffers
                    logger.debug("WRITER: Received shutdown signal. Flushing remaining buffers...")
                    try:
                        for table in list(buffers.keys()):
                            await flush(table)
                    except Exception as e:
                        logger.exception(f"WRITER: Error during shutdown flush: {e}")
                        raise
                    return

                # Add to buffer
                table = packet.table

                buffers[table].append(packet)
                previous_rows = buffer_rows[table]
                current_rows = previous_rows + len(packet.frame)
                buffer_rows[table] = current_rows

                # Log progress every chunk to assure user it's working
                chunk_rows = max(1, int(PROGRESS_LOG_CHUNK_ROWS))
                if (current_rows // chunk_rows) > (previous_rows // chunk_rows):
                    logger.debug(f"WRITER: Buffering {table}... {current_rows:,} / {threshold:,} rows")

                if current_rows >= threshold:
                    await flush(table)

            except asyncio.CancelledError:
                try:
                    for table in list(buffers.keys()):
                        await asyncio.shield(flush(table))
                except Exception as e:
                    logger.exception(f"WRITER: Error during cancellation flush: {e}")
                raise
            except Exception as e:
                async with result_lock:
                    if not getattr(e, "_already_reported", False):
                        result.errors.append(f"Writer:Unexpected:{e}")
                logger.exception("WRITER: Unexpected error")
                raise
            finally:
                pkt_q.task_done()

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
