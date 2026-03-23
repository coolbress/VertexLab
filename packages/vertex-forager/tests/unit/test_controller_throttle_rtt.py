from __future__ import annotations

import asyncio

import pytest

from vertex_forager.core.controller import FlowController


@pytest.mark.asyncio
async def test_throttle_rtt_excludes_queue_wait() -> None:
    ctrl = FlowController(requests_per_minute=10000, concurrency_limit=1)

    async def worker() -> None:
        async with ctrl.throttle():
            await asyncio.sleep(0.2)

    t1 = asyncio.create_task(worker())
    await asyncio.sleep(0)  # schedule second task while first holds the slot
    t2 = asyncio.create_task(worker())
    await asyncio.gather(t1, t2)

    rtt = float(ctrl._concurrency_limiter.rtt_ema)
    assert rtt >= 0.15
    assert rtt < 0.3
