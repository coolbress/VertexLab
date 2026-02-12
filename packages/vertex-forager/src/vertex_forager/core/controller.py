from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager

logger = logging.getLogger("vertex_forager.flow")


class GradientConcurrencyLimiter:
    """Adaptive Concurrency Controller based on Netflix's Gradient Algorithm.

    [What it limits]
    Limits the number of **Concurrent In-flight Requests** (Concurrency).
    It dynamically adjusts the `limit` (max concurrent requests) to find the system's optimal throughput without overloading it.

    [Theoretical Background]
    Based on **Little's Law** (L = λW) and **Netflix's Gradient Algorithm**.
    It calculates a gradient using the ratio of `min_rtt` (ideal no-load latency) to `observed_rtt` (current latency).
    - Gradient < 1 (RTT increased): System is overloaded -> Decrease limit.
    - Gradient ~ 1 (RTT stable): System is healthy -> Increase limit slightly (explore capacity).

    Formula: new_limit = old_limit * (min_rtt / current_rtt) + queue_size
    """

    def __init__(
        self,
        initial_limit: int,
        min_limit: int = 1,
        max_limit: int = 100,
        queue_size: int = 4,
        smoothing: float = 0.2,
    ) -> None:
        self.limit = float(initial_limit)
        self.min_limit = float(min_limit)
        self.max_limit = float(max_limit)
        self.queue_size = queue_size
        self.smoothing = smoothing

        self.inflight = 0
        self._condition = asyncio.Condition()

        # Metrics
        self.min_rtt = float("inf")
        self.rtt_ema = 0.0  # Exponential Moving Average of RTT
        self.window_start = time.monotonic()
        self.window_duration = 60.0  # Reset min_rtt every minute
        self._rtt_samples: deque[float] = deque(maxlen=100)

    async def acquire(self) -> None:
        """Acquire a concurrency slot."""
        async with self._condition:
            while self.inflight >= self.limit:
                await self._condition.wait()
            self.inflight += 1

    async def release(self, rtt: float) -> None:
        """Release a slot and update the concurrency limit based on RTT."""
        async with self._condition:
            self._update_limit(rtt)
            self.inflight -= 1
            self._condition.notify()

    def _update_limit(self, rtt: float) -> None:
        """Update limit using Gradient algorithm."""
        now = time.monotonic()

        # Reset min_rtt periodically to handle network changes
        if now - self.window_start > self.window_duration:
            self.min_rtt = rtt
            self.window_start = now
        else:
            self.min_rtt = min(self.min_rtt, rtt)

        # Update EMA RTT
        if self.rtt_ema == 0:
            self.rtt_ema = rtt
        else:
            self.rtt_ema = (self.rtt_ema * (1 - self.smoothing)) + (rtt * self.smoothing)

        # Calculate Gradient
        # If RTT increases, gradient < 1 -> limit decreases
        # If RTT is close to min_rtt, gradient ~ 1 -> limit increases (via queue_size)
        if self.rtt_ema > 0:
            gradient = self.min_rtt / self.rtt_ema
        else:
            gradient = 1.0

        # New Limit
        new_limit = self.limit * gradient + self.queue_size
        new_limit = max(self.min_limit, min(self.max_limit, new_limit))

        self.limit = new_limit


class GCRARateLimiter:
    """Async Rate Limiter implementing GCRA (Generic Cell Rate Algorithm).

    [What it limits]
    Limits the **Rate of Requests** over time (e.g., Requests Per Minute).
    It ensures the application complies with the API provider's usage quotas (Rate Limits).

    [Theoretical Background]
    **GCRA (Generic Cell Rate Algorithm)** is a sophisticated Leaky Bucket variant.
    Instead of using a counter reset window (which causes "thundering herd" at window boundaries),
    it tracks the **Theoretical Arrival Time (TAT)** for the next request.
    - If a request arrives before TAT, it waits (or is rejected).
    - Allows short **Bursts** up to a defined limit, but strictly enforces the long-term average rate.
    """

    def __init__(self, requests_per_minute: int, burst_limit: int | None = None) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")

        self._rpm = requests_per_minute
        self._lock = asyncio.Lock()

        self._emission_interval = 60.0 / self._rpm
        # Burst limit: how many requests can be made instantly
        self._burst_limit = float(burst_limit if burst_limit is not None else self._rpm)
        self._burst_offset = self._emission_interval * self._burst_limit

        self._tat = 0.0  # Theoretical Arrival Time

    async def acquire(self) -> None:
        """Acquire permission to proceed. Waits if necessary."""
        async with self._lock:
            now = time.monotonic()

            # TAT represents the time when the next request is allowed to finish
            # If TAT is in the past, reset it to now
            if self._tat < now:
                self._tat = now

            # Calculate new TAT
            increment = self._emission_interval
            new_tat = self._tat + increment

            # Allow burst: The allow_at time is new_tat - burst_offset
            allow_at = new_tat - self._burst_offset
            diff = allow_at - now

            if diff > 0:
                # Need to wait
                self._tat = new_tat
            else:
                self._tat = new_tat
                diff = 0

        if diff > 0:
            await asyncio.sleep(diff)


class FlowController:
    """Unified Flow Controller managing both Concurrency and Rate Limiting.

    Combines:
    1. GradientConcurrencyLimiter: To handle backpressure and optimize throughput based on latency.
    2. GCRARateLimiter: To strictly enforce API rate limits (RPM).
    """

    def __init__(
        self,
        requests_per_minute: int,
        concurrency_limit: int | None = None,
    ) -> None:
        # Calculate optimal concurrency using Little's Law if not provided
        if concurrency_limit is None:
            # L = λ * W
            # λ (Throughput) = RPM / 60
            # W (Avg Latency) ≈ 6.0s (Conservative estimate for high-latency financial APIs)
            # L (Concurrency) = (RPM / 60) * 6 = RPM / 10
            estimated = requests_per_minute // 10
            # Bounds: Min 10 (liveness), Max 50 (socket exhaustion prevention)
            concurrency_limit = max(10, min(50, estimated))
            logger.info(f"Auto-configured concurrency: {concurrency_limit} (based on {requests_per_minute} RPM)")

        self._rate_limiter = GCRARateLimiter(
            requests_per_minute=requests_per_minute,
            burst_limit=concurrency_limit,
        )
        self._concurrency_limiter = GradientConcurrencyLimiter(
            initial_limit=concurrency_limit,
            max_limit=concurrency_limit,
            min_limit=5,
        )

    @property
    def concurrency_limit(self) -> int:
        """Get the configured maximum concurrency limit."""
        return int(self._concurrency_limiter.max_limit)

    @asynccontextmanager
    async def throttle(self) -> None:
        """Context manager to acquire both rate limit and concurrency slot."""
        # 1. Acquire Rate Limit (may sleep)
        await self._rate_limiter.acquire()

        # 2. Acquire Concurrency Slot (may wait)
        start_time = time.monotonic()
        await self._concurrency_limiter.acquire()

        try:
            yield
        finally:
            # 3. Release Concurrency Slot with RTT feedback
            end_time = time.monotonic()
            rtt = end_time - start_time
            await self._concurrency_limiter.release(rtt)
