from __future__ import annotations

import httpx
import pytest

from vertex_forager.core.config import RetryConfig
from vertex_forager.core.retry import create_retry_controller


def _status_error(code: int) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "http://test")
    resp = httpx.Response(code, request=req)
    return httpx.HTTPStatusError("err", request=req, response=resp)


def _status_error_with_headers(code: int, headers: dict[str, str]) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "http://test")
    resp = httpx.Response(code, request=req, headers=headers)
    return httpx.HTTPStatusError("err", request=req, response=resp)


def _retry_state(exc: BaseException, attempt_number: int = 2) -> object:
    return type(
        "RetryState",
        (),
        {
            "attempt_number": attempt_number,
            "outcome": type("Outcome", (), {"exception": lambda self: exc})(),
        },
    )()


@pytest.mark.asyncio
async def test_retry_on_429_enabled():
    cfg = RetryConfig(
        max_attempts=3,
        base_backoff_s=0.01,
        max_backoff_s=0.02,
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
    cfg = RetryConfig(max_attempts=2, base_backoff_s=0.01, max_backoff_s=0.02, retry_status_codes=())
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


def test_removed_enable_http_status_retry_raises_error() -> None:
    with pytest.raises(ValueError, match="has been removed"):
        RetryConfig(enable_http_status_retry=False, retry_status_codes=(429, 503))


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


def test_wait_uses_retry_after_header(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = RetryConfig(max_attempts=3, base_backoff_s=1.0, max_backoff_s=30.0)
    controller = create_retry_controller(cfg)
    state = _retry_state(_status_error_with_headers(429, {"Retry-After": "7"}))
    monkeypatch.setattr("vertex_forager.core.retry.random.uniform", lambda low, high: 999.0)
    assert controller.wait(state) == 7.0


def test_wait_caps_retry_after_header_at_max_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = RetryConfig(max_attempts=3, base_backoff_s=1.0, max_backoff_s=5.0)
    controller = create_retry_controller(cfg)
    state = _retry_state(_status_error_with_headers(429, {"Retry-After": "60"}))
    monkeypatch.setattr("vertex_forager.core.retry.random.uniform", lambda low, high: 999.0)
    assert controller.wait(state) == 5.0


def test_wait_falls_back_to_jitter_without_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = RetryConfig(max_attempts=3, base_backoff_s=2.0, max_backoff_s=30.0)
    controller = create_retry_controller(cfg)
    state = _retry_state(_status_error_with_headers(429, {}))
    called: dict[str, tuple[float, float]] = {}

    def _uniform(low: float, high: float) -> float:
        called["args"] = (low, high)
        return 1.25

    monkeypatch.setattr("vertex_forager.core.retry.random.uniform", _uniform)
    assert controller.wait(state) == 1.25
    assert called["args"] == (0.0, 4.0)


def test_wait_retry_after_non_integer_falls_back_to_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = RetryConfig(max_attempts=3, base_backoff_s=2.0, max_backoff_s=30.0)
    controller = create_retry_controller(cfg)
    state = _retry_state(_status_error_with_headers(429, {"Retry-After": "0.5"}))
    called: dict[str, tuple[float, float]] = {}

    def _uniform(low: float, high: float) -> float:
        called["args"] = (low, high)
        return 1.5

    monkeypatch.setattr("vertex_forager.core.retry.random.uniform", _uniform)
    assert controller.wait(state) == 1.5
    assert called["args"] == (0.0, 4.0)


def test_wait_uses_equal_backoff_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = RetryConfig(max_attempts=3, base_backoff_s=2.0, max_backoff_s=30.0, backoff_mode="equal")
    controller = create_retry_controller(cfg)
    state = _retry_state(_status_error_with_headers(429, {}))
    called: dict[str, tuple[float, float]] = {}

    def _uniform(low: float, high: float) -> float:
        called["args"] = (low, high)
        return 0.5

    monkeypatch.setattr("vertex_forager.core.retry.random.uniform", _uniform)
    assert controller.wait(state) == 2.5
    assert called["args"] == (0.0, 2.0)
