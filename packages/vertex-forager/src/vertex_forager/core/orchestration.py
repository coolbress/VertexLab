from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any


async def enqueue_request_sentinels(
    *,
    req_q: asyncio.PriorityQueue[tuple[int, int, object | None]] | None,
    fetch_n: int,
    priority_sentinel: int,
    logger: Any,
) -> None:
    """Enqueue shutdown sentinels into the request queue.

    Adds `(priority_sentinel, 0, None)` up to `fetch_n` times so fetch workers
    can observe shutdown signaling and exit cleanly.

    Args:
        req_q: Target request priority queue or ``None`` when unavailable.
        fetch_n: Number of fetch workers/sentinels to enqueue.
        priority_sentinel: Priority value used for sentinel queue entries.
        logger: Logger used for best-effort debug diagnostics.

    Returns:
        None.

    Notes:
        - Uses non-blocking ``put_nowait`` in a loop.
        - Suppresses queue/operational errors and emits debug logs instead.
        - Stops early if queue insertion fails during the loop.
    """
    try:
        if req_q is None or fetch_n <= 0:
            return
        for _ in range(fetch_n):
            try:
                req_q.put_nowait((priority_sentinel, 0, None))
            except Exception as e:
                logger.debug("PIPELINE: Skipping req_q sentinel (%s)", e)
                break
    except Exception as e:
        logger.debug("PIPELINE: Failed to enqueue request sentinels during stop: %s", e, exc_info=True)


def schedule_packet_sentinels(
    *,
    pkt_q: asyncio.Queue[object | None] | None,
    writer_tasks: list[asyncio.Task[Any]] | None,
    logger: Any,
) -> tuple[int, list[asyncio.Task[Any]]]:
    try:
        if pkt_q is None:
            return 0, []
        task_list = writer_tasks or []
        active_writers = sum(1 for t in task_list if not t.done())
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


async def await_packet_sentinel_tasks(
    *,
    writer_set: set[asyncio.Task[Any]],
    sentinel_put_tasks: list[asyncio.Task[Any]],
    logger: Any,
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


async def cancel_non_writer_tasks(
    *,
    active_tasks: list[asyncio.Task[Any]],
    writer_set: set[asyncio.Task[Any]],
) -> None:
    other_tasks: list[asyncio.Task[Any]] = [t for t in active_tasks if t not in writer_set]
    for task in other_tasks:
        if not task.done():
            task.cancel()
    if other_tasks:
        await asyncio.gather(*other_tasks, return_exceptions=True)


async def await_writer_tasks(
    *,
    writer_set: set[asyncio.Task[Any]],
    logger: Any,
) -> None:
    if not writer_set:
        return
    try:
        await asyncio.gather(*writer_set, return_exceptions=True)
    except Exception:
        logger.debug("PIPELINE: Awaiting writer tasks raised but suppressed", exc_info=True)
