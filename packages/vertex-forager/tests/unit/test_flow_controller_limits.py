import asyncio
import time

import pytest
from vertex_forager.core.controller import (
    FlowController,
    GCRARateLimiter,
    GradientConcurrencyLimiter,
)


@pytest.mark.asyncio
async def test_gcra_enforces_rate_with_burst() -> None:
    # 60 RPM -> emission interval 1.0s; burst limit 2 allows 2 immediate acquires
    gcra = GCRARateLimiter(requests_per_minute=60, burst_limit=2)
    t0 = time.monotonic()
    # 4 acquires; first 2 immediate, next 2 spaced ~1s each (allow small jitter)
    for _ in range(4):
        await gcra.acquire()
    elapsed = time.monotonic() - t0
    # Expect at least about 2 seconds total wait minus tolerance
    assert elapsed >= 2.0 - 0.15


@pytest.mark.asyncio
async def test_gradient_limiter_bounds() -> None:
    limiter = GradientConcurrencyLimiter(
        initial_limit=3, min_limit=1, max_limit=5, queue_size=2, smoothing=0.2
    )
    await limiter.acquire()
    # Provide a baseline RTT then a larger RTT; ensure limit stays within bounds
    await limiter.release(0.1)
    await limiter.acquire()
    await limiter.release(0.8)
    assert 1.0 <= limiter.limit <= 5.0
    # Ensure limit is a float and non-zero
    assert isinstance(limiter.limit, float)
    assert limiter.limit > 0.0


def test_flow_controller_concurrency_property() -> None:
    fc = FlowController(requests_per_minute=120, concurrency_limit=8)
    assert fc.concurrency_limit == 8


@pytest.mark.asyncio
async def test_record_feedback_triggers_downshift() -> None:
    fc = FlowController(
        requests_per_minute=60,
        concurrency_limit=1,
        downshift_enabled=True,
        error_rate_threshold=0.2,
        rpm_floor=10,
        downshift_window_s=60,
    )
    for _ in range(11):
        fc.record_feedback(status_code=429)
    await asyncio.sleep(0.05)
    assert fc._effective_rpm < 60
    assert fc._effective_rpm >= 10


@pytest.mark.asyncio
async def test_record_feedback_respects_floor() -> None:
    fc = FlowController(
        requests_per_minute=60,
        concurrency_limit=1,
        downshift_enabled=True,
        error_rate_threshold=0.0,
        rpm_floor=20,
        downshift_window_s=1,
    )
    for _ in range(100):
        fc.record_feedback(status_code=429)
    await asyncio.sleep(0.1)
    assert fc._effective_rpm >= 20
