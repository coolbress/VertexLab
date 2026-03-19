from __future__ import annotations

import math
import time

import pytest
from vertex_forager.core.controller import GCRARateLimiter, GradientConcurrencyLimiter


@pytest.mark.asyncio
async def test_gradient_window_rollover_resets_min_rtt() -> None:
    limiter = GradientConcurrencyLimiter(
        initial_limit=3, min_limit=1, max_limit=5, queue_size=1, smoothing=0.2
    )
    await limiter.acquire()
    await limiter.release(0.2)
    old_min = limiter.min_rtt
    limiter.window_start = time.monotonic() - limiter.window_duration - 1.0
    await limiter.acquire()
    await limiter.release(0.5)
    assert math.isclose(limiter.min_rtt, 0.5, rel_tol=1e-6)
    assert limiter.min_rtt != old_min


@pytest.mark.asyncio
async def test_gradient_smoothing_and_clamp() -> None:
    limiter = GradientConcurrencyLimiter(
        initial_limit=2, min_limit=1, max_limit=3, queue_size=1, smoothing=0.5
    )
    await limiter.acquire()
    await limiter.release(0.4)
    # release(0.4) updates limiter.rtt_ema via EMA; the next release verifies smoothing
    await limiter.acquire()
    await limiter.release(0.6)
    ema2 = limiter.rtt_ema
    assert math.isclose(ema2, 0.5 * 0.4 + 0.5 * 0.6, rel_tol=1e-6)
    assert 1.0 <= limiter.limit <= 3.0


@pytest.mark.asyncio
async def test_gcra_interval_with_tolerance() -> None:
    gcra = GCRARateLimiter(requests_per_minute=120, burst_limit=2)
    t_marks = []
    for _ in range(6):
        await gcra.acquire()
        t_marks.append(time.monotonic())
    intervals = [t_marks[i] - t_marks[i - 1] for i in range(2, len(t_marks))]
    expected = 60.0 / 120.0
    lower_tol = 0.1
    upper_tol = 0.35
    for dt in intervals:
        assert expected - lower_tol <= dt <= expected + upper_tol
