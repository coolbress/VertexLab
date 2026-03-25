from __future__ import annotations

import httpx
import pytest

from vertex_forager.core.config import FetchJob, RequestSpec
from vertex_forager.core.retry import fetch_with_retry


class _Attempt:
    def __init__(self, attempt_number: int) -> None:
        self.retry_state = type("S", (), {"attempt_number": attempt_number})()

    def __enter__(self) -> _Attempt:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _RetryController:
    def __init__(self, attempts: list[_Attempt]) -> None:
        self._attempts = attempts

    def __aiter__(self) -> _RetryController:
        self._idx = 0
        return self

    async def __anext__(self) -> _Attempt:
        if self._idx >= len(self._attempts):
            raise StopAsyncIteration
        item = self._attempts[self._idx]
        self._idx += 1
        return item


class _Throttle:
    async def __aenter__(self) -> _Throttle:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _Controller:
    def throttle(self) -> _Throttle:
        return _Throttle()

    def record_feedback(self, **kwargs: object) -> None:
        pass


def _job() -> FetchJob:
    return FetchJob(
        provider="sharadar",
        dataset="price",
        symbol="AAPL",
        spec=RequestSpec(url="https://example.test", params={}),
    )


@pytest.mark.asyncio
async def test_fetch_with_retry_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vertex_forager.core.retry.create_retry_controller",
        lambda *_args, **_kwargs: _RetryController([_Attempt(1)]),
    )
    observed: list[float] = []
    stages: list[str] = []

    payload = await fetch_with_retry(
        job=_job(),
        retry_config=object(),
        controller=_Controller(),
        http_fetch=lambda _spec: _async_value(b"ok"),
        observe=lambda _name, val: observed.append(val),
        log_structured=lambda **kwargs: stages.append(str(kwargs.get("stage"))),
    )
    assert payload == b"ok"
    assert "http_start" in stages
    assert "http_end" in stages
    assert len(observed) == 1


@pytest.mark.asyncio
async def test_fetch_with_retry_logs_status_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vertex_forager.core.retry.create_retry_controller",
        lambda *_args, **_kwargs: _RetryController([_Attempt(1)]),
    )
    stages: list[str] = []
    request = httpx.Request("GET", "https://example.test")
    response = httpx.Response(500, request=request)

    async def _fail(_spec: object) -> bytes:
        raise httpx.HTTPStatusError("boom", request=request, response=response)

    with pytest.raises(httpx.HTTPStatusError):
        await fetch_with_retry(
            job=_job(),
            retry_config=object(),
            controller=_Controller(),
            http_fetch=_fail,
            observe=lambda _name, _val: None,
            log_structured=lambda **kwargs: stages.append(str(kwargs.get("stage"))),
        )
    assert any(stage.startswith("http_retry_reason:http_status_500") for stage in stages)


async def _async_value(value: bytes) -> bytes:
    return value
