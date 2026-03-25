from __future__ import annotations

import asyncio
import contextlib
import logging

import pytest

from vertex_forager.core.orchestration import (
    await_packet_sentinel_tasks,
    await_writer_tasks,
    cancel_non_writer_tasks,
    enqueue_request_sentinels,
    schedule_packet_sentinels,
)


@pytest.mark.asyncio
async def test_enqueue_request_sentinels_puts_expected_items() -> None:
    req_q: asyncio.PriorityQueue[tuple[int, int, object | None]] = asyncio.PriorityQueue()
    await enqueue_request_sentinels(
        req_q=req_q,
        fetch_n=3,
        priority_sentinel=99,
        logger=logging.getLogger("test"),
    )
    items = [req_q.get_nowait() for _ in range(3)]
    assert items == [(99, 0, None), (99, 0, None), (99, 0, None)]


@pytest.mark.asyncio
async def test_schedule_packet_sentinels_enqueues_none_for_active_writers() -> None:
    pkt_q: asyncio.Queue[object | None] = asyncio.Queue()
    writer_tasks = [asyncio.create_task(asyncio.sleep(0.01)) for _ in range(2)]
    try:
        active, put_tasks = schedule_packet_sentinels(
            pkt_q=pkt_q,
            writer_tasks=writer_tasks,
            logger=logging.getLogger("test"),
        )
        assert active == 2
        assert put_tasks == []
        assert pkt_q.get_nowait() is None
        assert pkt_q.get_nowait() is None
    finally:
        for task in writer_tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


@pytest.mark.asyncio
async def test_await_packet_sentinel_tasks_no_tasks_returns_false() -> None:
    timed_out = await await_packet_sentinel_tasks(
        writer_set=set(),
        sentinel_put_tasks=[],
        logger=logging.getLogger("test"),
    )
    assert timed_out is False


@pytest.mark.asyncio
async def test_cancel_non_writer_tasks_cancels_only_non_writers() -> None:
    writer = asyncio.create_task(asyncio.sleep(10))
    worker = asyncio.create_task(asyncio.sleep(10))
    try:
        await cancel_non_writer_tasks(
            active_tasks=[writer, worker],
            writer_set={writer},
        )
        assert writer.cancelled() is False
        assert worker.cancelled() is True
    finally:
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer


@pytest.mark.asyncio
async def test_await_writer_tasks_handles_empty_set() -> None:
    await await_writer_tasks(
        writer_set=set(),
        logger=logging.getLogger("test"),
    )
