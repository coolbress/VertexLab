from __future__ import annotations

import asyncio
import statistics

import pytest

from vertex_forager.core.controller import FlowController


@pytest.mark.asyncio
async def test_throttle_rtt_excludes_queue_wait() -> None:
    ctrl = FlowController(requests_per_minute=10000, concurrency_limit=1)

    async def worker() -> None:
        async with ctrl.throttle():
            await asyncio.sleep(0.2)

    t1 = asyncio.create_task(worker())
    await asyncio.sleep(0)
    t2 = asyncio.create_task(worker())
    await asyncio.gather(t1, t2)

    rtt = float(ctrl._concurrency_limiter.rtt_ema)
    assert rtt >= 0.15
    assert rtt < 0.3
    raw = list(ctrl._concurrency_limiter._rtt_samples)
    assert len(raw) >= 2
    assert max(raw) < 0.3
    assert statistics.median(raw) < 0.3
    assert min(raw) >= 0.15
