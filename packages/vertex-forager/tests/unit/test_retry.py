from __future__ import annotations

import httpx
import pytest

from vertex_forager.core.config import RetryConfig
from vertex_forager.core.retry import create_retry_controller


def _status_error(code: int) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "http://test")
    resp = httpx.Response(code, request=req)
    return httpx.HTTPStatusError("err", request=req, response=resp)


@pytest.mark.asyncio
async def test_retry_on_429_enabled():
    cfg = RetryConfig(
        max_attempts=3,
        base_backoff_s=0.01,
        max_backoff_s=0.02,
        enable_http_status_retry=True,
        retry_status_codes=(429, 503),
    )
    controller = create_retry_controller(cfg)
    attempts = 0
    async for attempt in controller:
        with attempt:
            attempts += 1
            if attempts < 2:
                raise _status_error(429)
            return
    pytest.fail("Expected early return after successful retry on 429")


@pytest.mark.asyncio
async def test_retry_on_503_enabled():
    cfg = RetryConfig(
        max_attempts=3,
        base_backoff_s=0.01,
        max_backoff_s=0.02,
        enable_http_status_retry=True,
        retry_status_codes=(429, 503),
    )
    controller = create_retry_controller(cfg)
    attempts = 0
    async for attempt in controller:
        with attempt:
            attempts += 1
            if attempts < 2:
                raise _status_error(503)
            return
    pytest.fail("Expected early return after successful retry on 503")


@pytest.mark.asyncio
async def test_no_retry_on_400():
    cfg = RetryConfig(
        max_attempts=2,
        base_backoff_s=0.01,
        max_backoff_s=0.02,
        enable_http_status_retry=True,
        retry_status_codes=(429, 503),
    )
    controller = create_retry_controller(cfg)
    attempts = 0
    async def _run() -> None:
        nonlocal attempts
        async for attempt in controller:
            with attempt:
                attempts += 1
                raise _status_error(400)
    with pytest.raises(httpx.HTTPStatusError, match=r"^err$"):
        await _run()
    assert attempts == 1


@pytest.mark.asyncio
async def test_disabled_http_status_retry():
    cfg = RetryConfig(
        max_attempts=2,
        base_backoff_s=0.01,
        max_backoff_s=0.02,
        enable_http_status_retry=False,
        retry_status_codes=(429, 503),
    )
    controller = create_retry_controller(cfg)
    attempts = 0
    async def _run() -> None:
        nonlocal attempts
        async for attempt in controller:
            with attempt:
                attempts += 1
                raise _status_error(429)
    with pytest.raises(httpx.HTTPStatusError, match=r"^err$"):
        await _run()
    assert attempts == 1


@pytest.mark.asyncio
async def test_retry_on_transport_error():
    cfg = RetryConfig(max_attempts=3, base_backoff_s=0.01, max_backoff_s=0.02)
    controller = create_retry_controller(cfg)
    attempts = 0
    async for attempt in controller:
        with attempt:
            attempts += 1
            if attempts < 2:
                req = httpx.Request("GET", "http://test")
                raise httpx.TransportError("connection failed", request=req)
            return
    pytest.fail("Expected early return after successful retry on transport error")


@pytest.mark.asyncio
async def test_backoff_sequence_exponential():
    cfg = RetryConfig(max_attempts=3, base_backoff_s=0.02, max_backoff_s=0.05)
    controller = create_retry_controller(cfg)
    import time
    starts: list[float] = []
    count = 0
    async for attempt in controller:
        starts.append(time.monotonic())
        with attempt:
            count += 1
            if count < 3:
                req = httpx.Request("GET", "http://test")
                raise httpx.TransportError("temporary", request=req)
            break
    assert len(starts) == 3
    assert count == 3
    d1 = starts[1] - starts[0]
    d2 = starts[2] - starts[1]
    # With Full Jitter, waits are uniformly drawn up to the exponential cap
    margin = 0.2
    assert d1 >= 0.0
    assert d1 <= cfg.base_backoff_s + margin
    assert d2 >= 0.0
    assert d2 <= min(cfg.max_backoff_s, cfg.base_backoff_s * 2) + margin


@pytest.mark.asyncio
async def test_retry_exhaustion_reraises_transport_error():
    cfg = RetryConfig(max_attempts=3, base_backoff_s=0.005, max_backoff_s=0.02)
    controller = create_retry_controller(cfg)
    attempts = 0
    async def _run() -> None:
        nonlocal attempts
        async for attempt in controller:
            with attempt:
                attempts += 1
                req = httpx.Request("GET", "http://test")
                raise httpx.TransportError("persistent failure", request=req)
    with pytest.raises(httpx.TransportError, match=r"^persistent failure$"):
        await _run()
    assert attempts == cfg.max_attempts


@pytest.mark.asyncio
async def test_backoff_cap_enforced_high_attempts():
    cfg = RetryConfig(max_attempts=6, base_backoff_s=0.02, max_backoff_s=0.05)
    controller = create_retry_controller(cfg)
    import time
    starts: list[float] = []
    count = 0
    async for attempt in controller:
        starts.append(time.monotonic())
        with attempt:
            count += 1
            if count < cfg.max_attempts:
                req = httpx.Request("GET", "http://test")
                raise httpx.TransportError("temporary", request=req)
            break
    # Verify cap (max_backoff_s) is respected even at high attempts where expo > cap
    intervals = [starts[i + 1] - starts[i] for i in range(len(starts) - 1)]
    margin = 0.2
    assert len(intervals) >= 5
    assert max(intervals) <= cfg.max_backoff_s + margin
