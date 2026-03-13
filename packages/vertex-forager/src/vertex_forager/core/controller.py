from __future__ import annotations

import asyncio
import logging
import time
import math
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from vertex_forager.constants import GRADIENT_WINDOW_S

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
        self.window_duration = GRADIENT_WINDOW_S
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
            self.rtt_ema = (self.rtt_ema * (1 - self.smoothing)) + (
                rtt * self.smoothing
            )

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

    def __init__(
        self, requests_per_minute: int, burst_limit: int | None = None
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")

        self._rpm = requests_per_minute
        self._lock = asyncio.Lock()

        self._emission_interval = 60.0 / self._rpm
        # Burst limit: how many requests can be made instantly
        self._burst_limit = float(burst_limit if burst_limit is not None else self._rpm)
        self._burst_offset = self._emission_interval * self._burst_limit

        self._tat = 0.0  # Theoretical Arrival Time

    async def set_rpm_async(self, rpm: int) -> None:
        """Safely update RPM under the same lock used by acquire()."""
        rpm = max(1, int(rpm))
        async with self._lock:
            self._rpm = rpm
            self._emission_interval = 60.0 / self._rpm
            self._burst_offset = self._emission_interval * self._burst_limit

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
        *,
        downshift_enabled: bool = False,
        downshift_window_s: int = 60,
        error_rate_threshold: float = 0.2,
        rpm_floor: int = 1,
        recovery_step: int = 5,
        healthy_window_s: int = 60,
    ) -> None:
        """Initialize the unified Flow Controller.
        
        Args:
            requests_per_minute: int — Allowed requests per minute (RPM).
            concurrency_limit: int | None — Optional max concurrent requests; if None, auto-computed.
            downshift_enabled: bool — Enable adaptive RPM downshift/recovery.
            downshift_window_s: int — Sliding window (seconds) for error-rate calculation.
            error_rate_threshold: float — Error/retry ratio threshold to trigger downshift.
            rpm_floor: int — Minimum RPM allowed during downshift.
            recovery_step: int — RPM increment applied during each healthy recovery step.
            healthy_window_s: int — Duration (seconds) of healthy period required to upshift.
        
        Raises:
            ValueError: If requests_per_minute <= 0.
        """
        # Calculate optimal concurrency using Little's Law if not provided
        if concurrency_limit is None:
            # L = λ * W
            # λ (Throughput) = RPM / 60
            # W (Avg Latency)
            from vertex_forager.constants import DEFAULT_AVG_LATENCY_S, CONCURRENCY_MIN, CONCURRENCY_MAX
            estimated = int((requests_per_minute / 60.0) * DEFAULT_AVG_LATENCY_S)
            concurrency_limit = max(CONCURRENCY_MIN, min(CONCURRENCY_MAX, estimated))
            logger.info(
                f"Auto-configured concurrency: {concurrency_limit} (based on {requests_per_minute} RPM)"
            )
        else:
            concurrency_limit = max(1, int(concurrency_limit))

        self._rate_limiter = GCRARateLimiter(
            requests_per_minute=requests_per_minute,
            burst_limit=concurrency_limit,
        )
        from vertex_forager.constants import GRADIENT_QUEUE_SIZE_DEFAULT, GRADIENT_SMOOTHING_DEFAULT
        self._concurrency_limiter = GradientConcurrencyLimiter(
            initial_limit=concurrency_limit,
            max_limit=concurrency_limit,
            min_limit=max(1, min(5, concurrency_limit)),
            queue_size=GRADIENT_QUEUE_SIZE_DEFAULT,
            smoothing=GRADIENT_SMOOTHING_DEFAULT,
        )
        self._rpm_ceiling = int(requests_per_minute)
        self._effective_rpm = int(requests_per_minute)
        self._downshift_enabled = bool(downshift_enabled)
        win = float(downshift_window_s)
        if not math.isfinite(win) or win < 1.0:
            win = 1.0
        if win > 3600.0:
            win = 3600.0
        self._window_s = win
        thr = float(error_rate_threshold)
        if not math.isfinite(thr):
            thr = 0.0
        self._error_threshold = min(1.0, max(0.0, thr))
        self._rpm_floor = int(max(1, rpm_floor))
        self._recovery_step = int(max(1, recovery_step))
        healthy = float(healthy_window_s)
        if not math.isfinite(healthy) or healthy < 1.0:
            healthy = 1.0
        if healthy > 3600.0:
            healthy = 3600.0
        self._healthy_window_s = healthy
        self._events: deque[tuple[float, bool]] = deque()
        self._last_error_ts = 0.0
        self._last_adjust_ts = 0.0
        self._min_sample_size = 10
        self._last_downshift_ts = 0.0
        self._last_upshift_ts = 0.0
        self._error_count = 0
    
    async def _safe_set_rpm(self, rpm: int) -> bool:
        try:
            await self._rate_limiter.set_rpm_async(rpm)
            return True
        except RuntimeError:
            logger.exception("FLOW_EVENT rpm_update_failed rpm=%d", rpm)
            return False
    
    async def _apply_downshift(self, *, prev: int, new: int, ratio: float) -> None:
        ok = await self._safe_set_rpm(new)
        if ok:
            self._effective_rpm = new
            applied = time.monotonic()
            logger.info(
                "FLOW_EVENT rpm_downshift from=%d to=%d err_ratio=%.3f window_s=%.0f",
                prev,
                self._effective_rpm,
                ratio,
                self._window_s,
            )
            self._last_adjust_ts = applied
            self._last_downshift_ts = applied
    
    async def _apply_upshift(self, *, prev: int, new: int) -> None:
        ok = await self._safe_set_rpm(new)
        if ok:
            self._effective_rpm = new
            applied = time.monotonic()
            logger.info("FLOW_EVENT rpm_upshift from=%d to=%d", prev, self._effective_rpm)
            self._last_adjust_ts = applied
            self._last_upshift_ts = applied

    @property
    def concurrency_limit(self) -> int:
        """Get the configured maximum concurrency limit."""
        return int(self._concurrency_limiter.max_limit)

    @asynccontextmanager
    async def throttle(self) -> AsyncGenerator[None, None]:
        """Context manager to acquire both rate limit and concurrency slot.
        
        Behavior:
            - Acquires GCRA rate limiter permission (may sleep).
            - Acquires GradientConcurrencyLimiter slot (may wait).
            - Releases slot with RTT feedback on exit to adapt limits.
        
        Raises:
            asyncio.CancelledError: If the enclosing task is cancelled while waiting.
        """
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

    def record_feedback(self, *, status_code: int | None = None, retried: bool = False) -> None:
        if not self._downshift_enabled:
            return
        now = time.monotonic()
        is_error = retried or (status_code in (429, 503))
        self._events.append((now, is_error))
        if is_error:
            self._error_count += 1
        cutoff = now - self._window_s
        while self._events and self._events[0][0] < cutoff:
            _, was_err = self._events.popleft()
            if was_err:
                self._error_count = max(0, self._error_count - 1)
        total = len(self._events)
        max_samples = max(1, int(math.ceil(self._effective_rpm * self._window_s / 60.0)))
        effective_min = min(self._min_sample_size, max_samples)
        if total < effective_min:
            ratio = 0.0
        else:
            ratio = (self._error_count / total) if total > 0 else 0.0
        if is_error:
            self._last_error_ts = now
        if (
            ratio >= self._error_threshold
            and self._effective_rpm > self._rpm_floor
            and (self._last_downshift_ts == 0.0 or (now - self._last_downshift_ts) >= self._window_s)
        ):
            new_rpm = max(self._rpm_floor, int(self._effective_rpm * 0.8))
            if new_rpm != self._effective_rpm:
                prev_guard = self._last_downshift_ts
                self._last_downshift_ts = now
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._apply_downshift(prev=self._effective_rpm, new=new_rpm, ratio=ratio))
                except Exception:
                    self._last_downshift_ts = prev_guard
                    logger.exception("FLOW_EVENT rpm_downshift_schedule_failed new_rpm=%d", new_rpm)
            return
        if (now - max(self._last_error_ts, self._last_adjust_ts) >= self._healthy_window_s) and self._effective_rpm < self._rpm_ceiling:
            new_rpm = min(self._rpm_ceiling, self._effective_rpm + self._recovery_step)
            if new_rpm != self._effective_rpm:
                prev_up_guard = self._last_upshift_ts
                self._last_upshift_ts = now
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._apply_upshift(prev=self._effective_rpm, new=new_rpm))
                except Exception:
                    self._last_upshift_ts = prev_up_guard
                    logger.exception("FLOW_EVENT rpm_upshift_schedule_failed new_rpm=%d", new_rpm)
