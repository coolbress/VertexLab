from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
import itertools
import time
from typing import Any

from vertex_forager.core.config import FetchJob, FramePacket, ParseResult, RunResult
from vertex_forager.core.errors import RunError


async def fetch_payload(
    *,
    worker_id: int,
    priority: int,
    job: FetchJob,
    fetch_with_retry: Callable[[FetchJob], Awaitable[bytes]],
    span: Callable[..., Any],
    observe: Callable[[str, float], None],
    log_structured: Callable[..., None],
    logger: Any,
) -> bytes:
    logger.debug("[Worker-%s] Processing job: %s (priority: %s)", worker_id, job.symbol, priority)
    log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="fetch_start")
    t_fetch_start = time.monotonic()
    with span("fetch", provider=job.provider, dataset=job.dataset, symbol=str(job.symbol)):
        payload = await fetch_with_retry(job)
    fetch_latency = time.monotonic() - t_fetch_start
    observe("fetch_duration_s", fetch_latency)
    log_structured(
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


async def parse_payload(
    *,
    job: FetchJob,
    payload: bytes,
    worker_id: int,
    parse_executor: Any,
    router_parse: Callable[..., ParseResult],
    span: Callable[..., Any],
    observe: Callable[[str, float], None],
    log_structured: Callable[..., None],
    logger: Any,
) -> ParseResult:
    log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage="parse_start")
    t1 = time.monotonic()
    loop = asyncio.get_running_loop()
    with span("parse", provider=job.provider, dataset=job.dataset, symbol=str(job.symbol)):
        parse_result = await loop.run_in_executor(
            parse_executor,
            partial(router_parse, job=job, payload=payload),
        )
    parse_latency = time.monotonic() - t1
    observe("parse_duration_s", parse_latency)
    logger.debug(
        "[Worker-%s] Parsed %s in %.3fs. Packets: %s, Next Jobs: %s",
        worker_id,
        job.symbol,
        parse_latency,
        len(parse_result.packets),
        len(parse_result.next_jobs),
    )
    log_structured(
        provider=job.provider,
        dataset=job.dataset,
        symbol=job.symbol,
        stage="parse_end",
        duration_s=parse_latency,
    )
    return parse_result


async def emit_packets_and_next_jobs(
    *,
    parse_result: ParseResult,
    req_q: asyncio.PriorityQueue[tuple[int, int, FetchJob | None]],
    pkt_q: asyncio.Queue[FramePacket | None],
    order_counter: itertools.count,
    worker_id: int,
    job: FetchJob,
    parse_executor: Any,
    normalize_packet: Callable[..., FramePacket],
    inc: Callable[[str, int], None],
    priority_pagination: int,
    pagination_max_burst: int | None,
    enqueue_pagination_job: Callable[[FetchJob], Awaitable[None]],
    logger: Any,
) -> None:
    for packet in parse_result.packets:
        loop = asyncio.get_running_loop()
        normalized_packet = await loop.run_in_executor(
            parse_executor,
            partial(normalize_packet, packet=packet),
        )
        await pkt_q.put(normalized_packet)
        inc("packets_emitted", 1)
    if parse_result.next_jobs:
        logger.debug(
            "[Worker-%s] Adding %s pagination jobs for %s",
            worker_id,
            len(parse_result.next_jobs),
            job.symbol,
        )
        for next_job in parse_result.next_jobs:
            if pagination_max_burst is None:
                await req_q.put((priority_pagination, next(order_counter), next_job))
            else:
                await enqueue_pagination_job(next_job)


async def record_worker_error(
    *,
    result: RunResult,
    result_lock: asyncio.Lock,
    job: FetchJob,
    exc: Exception,
    worker_id: int,
    stage: str,
    inc: Callable[[str, int], None],
    log_structured: Callable[..., None],
    logger: Any,
) -> None:
    async with result_lock:
        result.errors.append(RunError.from_exception(exc, job.provider, job.dataset, job.symbol))
    if stage == "error_unexpected":
        logger.exception("[Worker-%s] Unexpected error processing %s: %s", worker_id, job.symbol, exc)
    elif stage == "error_fetch":
        logger.error("[Worker-%s] Fetch exhausted for %s: %s", worker_id, job.symbol, exc)
    else:
        logger.error("[Worker-%s] Error processing %s: %s", worker_id, job.symbol, exc)
    inc("errors_total", 1)
    log_structured(provider=job.provider, dataset=job.dataset, symbol=job.symbol, stage=stage)
