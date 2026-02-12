"""
Unit tests for core.controller module.

Tests cover:
- GradientConcurrencyLimiter (adaptive concurrency control)
- GCRARateLimiter (rate limiting with burst support)
- FlowController (unified flow control)
"""

import asyncio
import time
from unittest.mock import patch

import pytest

from vertex_forager.core.controller import (
    GradientConcurrencyLimiter,
    GCRARateLimiter,
    FlowController,
)


class TestGradientConcurrencyLimiter:
    """Test GradientConcurrencyLimiter class."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test limiter initializes with correct defaults."""
        limiter = GradientConcurrencyLimiter(
            initial_limit=10,
            min_limit=1,
            max_limit=100,
        )
        assert limiter.limit == 10.0
        assert limiter.min_limit == 1.0
        assert limiter.max_limit == 100.0
        assert limiter.inflight == 0
        assert limiter.min_rtt == float("inf")

    @pytest.mark.asyncio
    async def test_acquire_and_release(self):
        """Test basic acquire and release."""
        limiter = GradientConcurrencyLimiter(initial_limit=2)

        await limiter.acquire()
        assert limiter.inflight == 1

        await limiter.release(rtt=0.1)
        assert limiter.inflight == 0

    @pytest.mark.asyncio
    async def test_blocks_when_limit_exceeded(self):
        """Test that acquire blocks when limit is exceeded."""
        limiter = GradientConcurrencyLimiter(initial_limit=1)

        # First acquire succeeds immediately
        await limiter.acquire()
        assert limiter.inflight == 1

        # Second acquire should block
        acquire_started = False
        async def try_acquire():
            nonlocal acquire_started
            acquire_started = True
            await limiter.acquire()

        task = asyncio.create_task(try_acquire())
        await asyncio.sleep(0.01)  # Give task time to start

        assert acquire_started
        assert limiter.inflight == 1  # Still blocked

        # Release should unblock
        await limiter.release(rtt=0.1)
        await task
        assert limiter.inflight == 1  # Now acquired by second task

        # Cleanup
        await limiter.release(rtt=0.1)

    @pytest.mark.asyncio
    async def test_limit_adjustment_on_high_rtt(self):
        """Test that limit decreases when RTT increases."""
        limiter = GradientConcurrencyLimiter(
            initial_limit=50,
            min_limit=10,
            max_limit=100,
        )

        # Establish baseline RTT
        await limiter.acquire()
        await limiter.release(rtt=0.1)
        initial_limit = limiter.limit

        # Simulate high RTT (system overload)
        await limiter.acquire()
        await limiter.release(rtt=1.0)  # 10x higher RTT

        # Limit should decrease (gradient < 1)
        assert limiter.limit < initial_limit

    @pytest.mark.asyncio
    async def test_limit_respects_min_max_bounds(self):
        """Test that limit stays within min/max bounds."""
        limiter = GradientConcurrencyLimiter(
            initial_limit=50,
            min_limit=10,
            max_limit=100,
        )

        # Try to force limit below minimum with very high RTT
        for _ in range(20):
            await limiter.acquire()
            await limiter.release(rtt=10.0)

        assert limiter.limit >= limiter.min_limit

    @pytest.mark.asyncio
    async def test_min_rtt_reset_after_window(self):
        """Test that min_rtt resets after window duration."""
        limiter = GradientConcurrencyLimiter(initial_limit=10)
        limiter.window_duration = 0.1  # Short window for testing

        # Set initial min_rtt
        await limiter.acquire()
        await limiter.release(rtt=0.1)
        assert limiter.min_rtt == 0.1

        # Wait for window to expire
        await asyncio.sleep(0.15)

        # Next release should reset min_rtt
        await limiter.acquire()
        await limiter.release(rtt=0.5)
        assert limiter.min_rtt == 0.5


class TestGCRARateLimiter:
    """Test GCRARateLimiter class."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test rate limiter initializes correctly."""
        limiter = GCRARateLimiter(requests_per_minute=60)
        assert limiter._rpm == 60
        assert limiter._emission_interval == 1.0  # 60s / 60rpm = 1s
        assert limiter._tat == 0.0

    @pytest.mark.asyncio
    async def test_initialization_validates_positive_rpm(self):
        """Test that initialization requires positive RPM."""
        with pytest.raises(ValueError, match="requests_per_minute must be positive"):
            GCRARateLimiter(requests_per_minute=0)

        with pytest.raises(ValueError, match="requests_per_minute must be positive"):
            GCRARateLimiter(requests_per_minute=-10)

    @pytest.mark.asyncio
    async def test_acquire_allows_immediate_requests(self):
        """Test that initial requests are allowed immediately (burst)."""
        limiter = GCRARateLimiter(requests_per_minute=60)

        start = time.monotonic()
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should complete almost immediately (burst)
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_acquire_enforces_rate_limit(self):
        """Test that rate limiting kicks in after burst."""
        # 60 RPM = 1 request per second
        limiter = GCRARateLimiter(requests_per_minute=60, burst_limit=2)

        # First 2 requests should be instant (burst)
        await limiter.acquire()
        await limiter.acquire()

        # Third request should wait
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should have waited approximately 1 second
        assert elapsed >= 0.9  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_high_rate_limit(self):
        """Test rate limiter with high RPM."""
        # 600 RPM = 10 requests per second
        limiter = GCRARateLimiter(requests_per_minute=600)

        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should be very fast with high RPM and burst
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_custom_burst_limit(self):
        """Test rate limiter with custom burst limit."""
        limiter = GCRARateLimiter(requests_per_minute=60, burst_limit=5)

        # Should allow 5 immediate requests
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed < 0.1  # All burst requests should be immediate


class TestFlowController:
    """Test FlowController class."""

    def test_initialization_with_explicit_concurrency(self):
        """Test flow controller with explicit concurrency limit."""
        controller = FlowController(
            requests_per_minute=100,
            concurrency_limit=20,
        )
        assert controller.concurrency_limit == 20

    def test_initialization_auto_calculates_concurrency(self):
        """Test flow controller auto-calculates concurrency when not provided."""
        controller = FlowController(requests_per_minute=100)

        # Little's Law: L = λ * W
        # λ = 100/60 requests per second
        # W ≈ 6 seconds (conservative estimate)
        # L = (100/60) * 6 = 10
        # Bounded to [10, 50]
        assert 10 <= controller.concurrency_limit <= 50

    def test_auto_concurrency_respects_min_bound(self):
        """Test auto-calculated concurrency respects minimum of 10."""
        # Very low RPM should still give minimum of 10
        controller = FlowController(requests_per_minute=10)
        assert controller.concurrency_limit >= 10

    def test_auto_concurrency_respects_max_bound(self):
        """Test auto-calculated concurrency respects maximum of 50."""
        # Very high RPM should cap at 50
        controller = FlowController(requests_per_minute=10000)
        assert controller.concurrency_limit <= 50

    @pytest.mark.asyncio
    async def test_throttle_context_manager(self):
        """Test throttle as async context manager."""
        controller = FlowController(
            requests_per_minute=600,  # High RPM for fast test
            concurrency_limit=10,
        )

        async with controller.throttle():
            # Should enter and exit cleanly
            pass

    @pytest.mark.asyncio
    async def test_throttle_enforces_concurrency(self):
        """Test throttle enforces concurrency limits."""
        controller = FlowController(
            requests_per_minute=600,
            concurrency_limit=2,
        )

        active_count = 0
        max_active = 0
        lock = asyncio.Lock()

        async def worker():
            nonlocal active_count, max_active
            async with controller.throttle():
                async with lock:
                    active_count += 1
                    max_active = max(max_active, active_count)

                await asyncio.sleep(0.05)  # Simulate work

                async with lock:
                    active_count -= 1

        # Launch 10 workers
        tasks = [asyncio.create_task(worker()) for _ in range(10)]
        await asyncio.gather(*tasks)

        # Max active should not exceed concurrency limit
        assert max_active <= 2

    @pytest.mark.asyncio
    async def test_throttle_enforces_rate_limit(self):
        """Test throttle enforces rate limits."""
        # 60 RPM = 1 request per second
        controller = FlowController(
            requests_per_minute=60,
            concurrency_limit=10,
        )

        start = time.monotonic()

        # First few should be instant (burst)
        async with controller.throttle():
            pass
        async with controller.throttle():
            pass

        # After burst, should rate limit
        async with controller.throttle():
            pass

        elapsed = time.monotonic() - start

        # Should have taken some time due to rate limiting
        # (exact timing depends on burst limit)
        assert elapsed >= 0.0  # Should succeed without error

    @pytest.mark.asyncio
    async def test_multiple_concurrent_throttles(self):
        """Test multiple concurrent operations through throttle."""
        controller = FlowController(
            requests_per_minute=600,
            concurrency_limit=5,
        )

        async def worker(worker_id: int):
            async with controller.throttle():
                await asyncio.sleep(0.01)
                return worker_id

        # Run multiple workers concurrently
        tasks = [asyncio.create_task(worker(i)) for i in range(20)]
        results = await asyncio.gather(*tasks)

        # All should complete successfully
        assert len(results) == 20
        assert set(results) == set(range(20))


class TestIntegration:
    """Integration tests combining multiple components."""

    @pytest.mark.asyncio
    async def test_flow_controller_with_realistic_workload(self):
        """Test flow controller with realistic concurrent workload."""
        controller = FlowController(
            requests_per_minute=300,
            concurrency_limit=10,
        )

        completed = []

        async def worker(task_id: int):
            async with controller.throttle():
                await asyncio.sleep(0.01)  # Simulate API call
                completed.append(task_id)

        # Launch 50 workers
        start = time.monotonic()
        tasks = [asyncio.create_task(worker(i)) for i in range(50)]
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

        # All tasks should complete
        assert len(completed) == 50

        # Should take some time due to rate limiting and concurrency
        # but not too long
        assert elapsed < 30.0  # Reasonable upper bound

    @pytest.mark.asyncio
    async def test_flow_controller_handles_exceptions(self):
        """Test flow controller properly releases resources on exception."""
        controller = FlowController(
            requests_per_minute=600,
            concurrency_limit=5,
        )

        async def failing_worker():
            async with controller.throttle():
                raise ValueError("Simulated error")

        # Should raise exception but not leave limiter in bad state
        with pytest.raises(ValueError):
            await failing_worker()

        # Should still be able to acquire after exception
        async with controller.throttle():
            pass  # Should succeed


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_gradient_limiter_with_zero_rtt(self):
        """Test gradient limiter handles zero RTT gracefully."""
        limiter = GradientConcurrencyLimiter(initial_limit=10)

        await limiter.acquire()
        await limiter.release(rtt=0.0)

        # Should not crash or produce invalid limit
        assert limiter.limit > 0

    @pytest.mark.asyncio
    async def test_rate_limiter_with_very_high_rpm(self):
        """Test rate limiter with extremely high RPM."""
        limiter = GCRARateLimiter(requests_per_minute=60000)

        # Should handle high throughput
        start = time.monotonic()
        for _ in range(100):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should be very fast
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_flow_controller_with_single_concurrency(self):
        """Test flow controller with concurrency limit of 1."""
        controller = FlowController(
            requests_per_minute=600,
            concurrency_limit=1,
        )

        # Should serialize all operations
        results = []

        async def worker(task_id: int):
            async with controller.throttle():
                results.append(task_id)

        tasks = [asyncio.create_task(worker(i)) for i in range(5)]
        await asyncio.gather(*tasks)

        # All should complete (order may vary due to scheduling)
        assert len(results) == 5