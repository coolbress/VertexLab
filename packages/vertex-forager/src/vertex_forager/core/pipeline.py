from __future__ import annotations

import asyncio
import inspect
import logging
import time
import itertools
import functools
from typing import Any, TYPE_CHECKING
from collections.abc import Iterable, Sequence, Callable

import polars as pl

from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.config import EngineConfig, FetchJob, FramePacket, RunResult, ParseResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.retry import create_retry_controller
from vertex_forager.routers.base import BaseRouter
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.writers.base import BaseWriter

if TYPE_CHECKING:
    from vertex_forager.writers.memory import InMemoryBufferWriter

try:
    from vertex_forager.writers.memory import InMemoryBufferWriter
except ImportError:
    InMemoryBufferWriter = None

logger = logging.getLogger("vertex_forager.debug")

Symbols = Sequence[str]
Packets = Iterable[FramePacket]


class VertexForager:
    """High-performance asynchronous data pipeline engine.

    This class orchestrates the end-to-end data collection process using a
    Producer-Consumer architecture with asyncio. It manages three main stages:
    1.  **Job Generation (Producer)**: Iterates over the provider's job list.
    2.  **Data Fetching (Fetch Workers)**: Executes HTTP requests and parses responses.
    3.  **Data Writing (Writer Workers)**: Writes processed data to storage.

    Architecture:
        [Router] --(Jobs)--> [Req Queue] --(Fetch Workers)--> [Pkt Queue] --(Writer Workers)--> [Storage]
                                    |                              |
                            [HttpExecutor]                   [SchemaMapper]

    The pipeline is designed for maximum throughput while respecting rate limits
    and handling errors gracefully.
    """

    # Configurable flush threshold
    # Increased to 500k to allow better batching for large packets (125k rows each)
    FLUSH_THRESHOLD_ROWS = 500_000

    def __init__(
        self,
        *,
        router: BaseRouter,
        http: HttpExecutor,
        writer: BaseWriter,
        mapper: SchemaMapper,
        config: EngineConfig,
        controller: FlowController,
    ) -> None:
        self._router = router
        self._http = http
        self._writer = writer
        self._mapper = mapper
        self._config = config
        self.controller = controller
        
        # Validate configuration
        self._config.validate()
        
        # Optimization: Disable intermediate flushing for In-Memory Writer
        # Since InMemoryBufferWriter just stores frames in a list, we can 
        # avoid the overhead of intermediate merges by setting threshold to infinity.
        # This allows the worker to collect ALL frames and perform a SINGLE merge at the end.
        self._flush_threshold = config.flush_threshold_rows
        
        if InMemoryBufferWriter is not None and isinstance(writer, InMemoryBufferWriter):
            # Override instance config (not the global config object)
            # We treat 1 billion rows as effectively infinite for memory buffer
            self._flush_threshold = 1_000_000_000 
            logger.debug("PIPELINE: Detected InMemoryBufferWriter. Disabled intermediate flushing.")

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
        2.  Spawn `_sink_worker` tasks (consumers of processed data).
        3.  Spawn `_fetch_worker` tasks (consumers of jobs, producers of data).
        4.  Spawn `_producer` task (generator of jobs).
        5.  Wait for all tasks to complete in order:
            - Producer finishes -> Queue join.
            - Fetchers finish -> Queue join.
            - Writer workers finish.
        6.  Aggregate results and handle cleanup.

        Args:
            dataset: Name of the dataset to fetch (e.g., "price").
            symbols: List of symbols to fetch, or None for all.
            on_progress: Optional callback to update progress bar (called on job completion).
            **kwargs: Additional arguments passed to the router's generate_jobs method.

        Returns:
            RunResult: Summary of the run including metrics and errors.
        """
        # PriorityQueue to prioritize pagination (next jobs) over new jobs
        # Tuple structure: (priority, order, job)
        # Priority: 0=NextJob, 10=NewJob, 999=Sentinel
        req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]] = asyncio.PriorityQueue(
            maxsize=self._config.queue_max
        )
        pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue(
            maxsize=self._config.queue_max
        )

        result = RunResult(provider=self._router.provider)
        result_lock = asyncio.Lock()

        writer_tasks = [
            asyncio.create_task(
                self._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock),
                name=f"vertex-forager:writer:{i}",
            )
            for i in range(1)  # Always single writer for DuckDB
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

        try:
            logger.info("PIPELINE: Starting producer task...")
            await producer_task
            logger.info("PIPELINE: Producer completed, waiting for request queue...")
            await req_q.join()
            logger.info("PIPELINE: Request queue joined, sending sentinel signals...")
            for _ in range(self.controller.concurrency_limit):
                # Sentinel with lowest priority (highest number)
                await req_q.put((999, 0, None))
            logger.info("PIPELINE: Waiting for fetch tasks to complete...")
            await asyncio.gather(*fetch_tasks)

            logger.info("PIPELINE: Fetch tasks completed, waiting for packet queue...")
            await pkt_q.join()
            for _ in writer_tasks:
                await pkt_q.put(None)
            logger.info("PIPELINE: Waiting for writer tasks to complete...")
            await asyncio.gather(*writer_tasks)

            # Flush any buffered data in the writer
            logger.info("PIPELINE: Flushing writer buffer...")
            await self._writer.flush()

            logger.info(f"PIPELINE: Run completed. Total errors: {len(result.errors)}")
            return result
        finally:
            for task in [producer_task, *fetch_tasks, *writer_tasks]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                producer_task, *fetch_tasks, *writer_tasks, return_exceptions=True
            )

    async def _producer(self, *, req_q: asyncio.PriorityQueue, dataset: str, symbols: Symbols | None, **kwargs: object) -> None:
        """Generate fetch jobs and push them to the request queue.

        Iterates through the provider's `generate_jobs` generator. Once all jobs are
        enqueued, fetch workers may enqueue additional jobs (e.g., pagination).
        """
        counter = itertools.count()
        job_count = 0
        logger.info(f"PRODUCER: Starting job generation for dataset={dataset}, symbols={len(symbols) if symbols else 'all'}")
        
        async for job in self._router.generate_jobs(dataset=dataset, symbols=symbols, **kwargs):
            # Priority 10 for initial jobs
            await req_q.put((10, next(counter), job))
            job_count += 1
            if job_count % 100 == 0:
                logger.debug(f"PRODUCER: Generated {job_count} jobs so far...")
        
        logger.info(f"PRODUCER: Completed job generation. Total jobs: {job_count}")

    async def _fetch_worker(
        self,
        worker_id: int,
        *,
        req_q: asyncio.PriorityQueue,
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
        async def noop_handler(job: FetchJob, payload: bytes | None, exc: Exception | None, parse_result: ParseResult | None) -> None:
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
                    parse_result: ParseResult | None
                ) -> None:
                    kwargs = {"job": job, "payload": payload, "exc": exc}
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
        while True:
            priority, _, job = await req_q.get()
            job_count += 1
            if job_count % 100 == 0:
                logger.debug(f"[Worker-{worker_id}] Processed {job_count} jobs so far...")
                
            if job is None:
                logger.debug(f"[Worker-{worker_id}] Received sentinel, shutting down. Total jobs processed: {job_count}")
                req_q.task_done()
                return

            payload: bytes | None = None
            worker_exc: Exception | None = None
            parse_result: ParseResult | None = None
            try:
                # Log Fetch Start
                logger.debug(f"[Worker-{worker_id}] Processing job: {job.symbol} (priority: {priority})")
                
                t_fetch_start = time.monotonic()
                payload = await self._fetch_with_retry(job)
                t_fetch_end = time.monotonic()
                fetch_latency = t_fetch_end - t_fetch_start
                
                t1 = time.monotonic()
                logger.debug(f"[Worker-{worker_id}] Fetched {job.symbol} ({len(payload) if payload else 0} bytes) in {fetch_latency:.3f}s")
                
                # Offload CPU-bound parsing to a thread pool
                loop = asyncio.get_running_loop()
                parse_result = await loop.run_in_executor(
                    None,  # Use default ThreadPoolExecutor
                    functools.partial(self._router.parse, job=job, payload=payload)
                )
                t2 = time.monotonic()
                logger.debug(f"[Worker-{worker_id}] Parsed {job.symbol} in {t2 - t1:.3f}s. Packets: {len(parse_result.packets)}, Next Jobs: {len(parse_result.next_jobs)}")

                for packet in parse_result.packets:
                    # Normalize packet schema (enforce types, fill missing cols)
                    # Also CPU-bound, so we offload it
                    normalized_packet = await loop.run_in_executor(
                        None,
                        functools.partial(self._mapper.normalize, packet=packet)
                    )
                    await pkt_q.put(normalized_packet)

                if parse_result.next_jobs:
                    logger.debug(f"[Worker-{worker_id}] Adding {len(parse_result.next_jobs)} pagination jobs for {job.symbol}")
                    for next_job in parse_result.next_jobs:
                        # Priority 0 for derived jobs (e.g., pagination, detailed info) to complete logical units first.
                        # This implements a generic Depth-First Fetching strategy.
                        # Use monotonic time as tie breaker.
                        await req_q.put((0, time.monotonic_ns(), next_job))

            except Exception as exc:
                worker_exc = exc
                async with result_lock:
                    result.errors.append(f"{job.provider}:{job.dataset}:{job.symbol}:{exc}")
                logger.error(f"[Worker-{worker_id}] Error processing {job.symbol}: {exc}")
            finally:
                req_q.task_done()
                await handler(job, payload, worker_exc, parse_result)

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
        buffers: dict[str, list[FramePacket]] = {}
        buffer_rows: dict[str, int] = {}
        
        # Use config value
        threshold = self._flush_threshold
        logger.debug(f"WRITER: Adaptive bulk writing enabled. Threshold={threshold} rows")

        async def flush(table: str) -> None:
            packets = buffers.get(table, [])
            if not packets:
                return
            
            try:
                # Merge frames
                frames = [p.frame for p in packets]
                if not frames:
                    return
                    
                merged_frame = pl.concat(frames)
                
                # Create merged packet (use metadata from the first packet)
                first = packets[0]
                merged_packet = FramePacket(
                    provider=first.provider,
                    table=first.table,
                    frame=merged_frame,
                    observed_at=first.observed_at,
                    context=first.context,
                )
                
                logger.debug(f"WRITER: Flushing {len(packets)} packets ({len(merged_frame)} rows) for {table}")
                write_result = await self._writer.write(merged_packet)
                
                async with result_lock:
                    result.tables[write_result.table] = result.tables.get(write_result.table, 0) + write_result.rows
                
            except Exception as e:
                async with result_lock:
                    result.errors.append(f"Writer:{e}")
                logger.error(f"WRITER: Error flushing {table}: {e}")
            finally:
                # Clear buffer regardless of success
                buffers[table] = []
                buffer_rows[table] = 0

        while True:
            packet = await pkt_q.get()
            
            if packet is None:
                # Flush all remaining buffers
                logger.debug("WRITER: Received shutdown signal. Flushing remaining buffers...")
                for table in list(buffers.keys()):
                    await flush(table)
                pkt_q.task_done()
                return
            
            # Add to buffer
            table = packet.table
            if table not in buffers:
                buffers[table] = []
                buffer_rows[table] = 0
            
            buffers[table].append(packet)
            previous_rows = buffer_rows[table]
            current_rows = previous_rows + len(packet.frame)
            buffer_rows[table] = current_rows
            
            # Log progress every 100k rows to assure user it's working
            if (current_rows // 100_000) > (previous_rows // 100_000):
                logger.debug(f"WRITER: Buffering {table}... {current_rows:,} / {threshold:,} rows")
            
            # Check threshold
            if buffer_rows[table] >= threshold:
                await flush(table)
            
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
        retry_controller = create_retry_controller(self._config.retry)

        async for attempt in retry_controller:
            with attempt:
                async with self.controller.throttle():
                    return await self._http.fetch(job.spec)
