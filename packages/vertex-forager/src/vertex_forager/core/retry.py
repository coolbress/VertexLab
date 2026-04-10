"""Retry strategies using tenacity."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
import logging
import random
import time
from typing import TYPE_CHECKING, Protocol

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
)

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from vertex_forager.core.config import FetchJob, RequestSpec, RetryConfig

from vertex_forager.exceptions import FetchError

logger = logging.getLogger("vertex_forager.retry")


def create_retry_controller(
    config: RetryConfig,
    *,
    idempotent: bool = True,
    log_level: int = logging.WARNING,
    retry_on: tuple[type[Exception], ...] = (httpx.TransportError,),
) -> AsyncRetrying:
    """Create a tenacity AsyncRetrying controller from configuration.

    Args:
        config: Retry configuration.
        log_level: Logging level for retry attempts.
        retry_on: Tuple of exception types to retry on.
                Defaults to (httpx.TransportError,).

    Returns:
        AsyncRetrying: Configured retry controller.
    """

    def _should_retry(exc: BaseException) -> bool:
        if isinstance(exc, retry_on):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            resp = getattr(exc, "response", None)
            status = getattr(resp, "status_code", None)
            if status in set(config.retry_status_codes):
                return True
        return False

    def _wait_capped(retry_state: RetryCallState) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, httpx.HTTPStatusError):
            retry_after = exc.response.headers.get("Retry-After")
            if retry_after:
                try:
                    retry_after_int = int(retry_after)
                    return max(0.0, min(config.max_backoff_s, float(retry_after_int)))
                except ValueError:
                    pass
        att = retry_state.attempt_number
        expo = config.base_backoff_s * (2 ** max(0, att - 1))
        cap = max(0.0, min(config.max_backoff_s, expo))
        if cap <= 0.0:
            return 0.0
        if config.backoff_mode == "equal":
            half_cap = cap / 2.0
            return float(half_cap + random.uniform(0.0, half_cap))  # noqa: S311
        return float(random.uniform(0.0, cap))  # noqa: S311

    # For non-idempotent requests, guard against repeats by forcing a single attempt.
    stop_policy = stop_after_attempt(1 if not idempotent else config.max_attempts)

    return AsyncRetrying(
        stop=stop_policy,
        wait=_wait_capped,
        retry=retry_if_exception(_should_retry),
        before_sleep=before_sleep_log(logger, log_level),
        reraise=True,
    )


class RetryAttempt(Protocol):
    retry_state: object | None

    def __enter__(self) -> object: ...
    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool: ...


class RetryController(Protocol):
    def __aiter__(self) -> AsyncIterator[RetryAttempt]: ...


class FeedbackController(Protocol):
    def throttle(self) -> AbstractAsyncContextManager[object]: ...


HttpFetch = Callable[["RequestSpec"], Awaitable[bytes]]
ObserveFunc = Callable[[str, float], None]
LogStructuredFunc = Callable[..., None]


class RetryExecutor:
    def __init__(
        self,
        *,
        retry_config: RetryConfig,
        controller: FeedbackController,
        http_fetch: HttpFetch,
        observe: ObserveFunc,
        log_structured: LogStructuredFunc,
    ) -> None:
        self._retry_config = retry_config
        self._controller = controller
        self._http_fetch = http_fetch
        self._observe = observe
        self._log_structured = log_structured

    async def fetch(self, job: FetchJob) -> bytes:
        return await fetch_with_retry(
            job=job,
            retry_config=self._retry_config,
            controller=self._controller,
            http_fetch=self._http_fetch,
            observe=self._observe,
            log_structured=self._log_structured,
        )


async def fetch_with_retry(
    *,
    job: FetchJob,
    retry_config: RetryConfig,
    controller: FeedbackController,
    http_fetch: HttpFetch,
    observe: ObserveFunc,
    log_structured: LogStructuredFunc,
) -> bytes:
    retry_controller = create_retry_controller(retry_config, idempotent=job.spec.idempotent)
    async for attempt in retry_controller:
        with attempt:
            state = getattr(attempt, "retry_state", None)
            att_no = getattr(state, "attempt_number", None) if state is not None else None
            async with controller.throttle():
                t0 = time.monotonic()
                log_structured(
                    provider=job.provider,
                    dataset=job.dataset,
                    symbol=job.symbol,
                    stage="http_start",
                    attempt=att_no,
                )
                try:
                    resp = await http_fetch(job.spec)
                    rf = getattr(controller, "record_feedback", None)
                    if callable(rf):
                        rf(status_code=200, retried=bool(att_no and att_no > 1))
                    t1 = time.monotonic()
                    dur = t1 - t0
                    observe("http_duration_s", dur)
                    log_structured(
                        provider=job.provider,
                        dataset=job.dataset,
                        symbol=job.symbol,
                        stage="http_end",
                        attempt=att_no,
                        duration_s=dur,
                    )
                    return resp
                except Exception as exc:
                    reason = "error"
                    if isinstance(exc, httpx.HTTPStatusError):
                        resp0 = getattr(exc, "response", None)
                        sc = getattr(resp0, "status_code", None)
                        reason = f"http_status_{sc}"
                        rf = getattr(controller, "record_feedback", None)
                        if callable(rf):
                            rf(status_code=sc, retried=True)
                    elif isinstance(exc, httpx.TransportError):
                        reason = "transport_error"
                        rf = getattr(controller, "record_feedback", None)
                        if callable(rf):
                            rf(status_code=None, retried=True)
                    else:
                        reason = type(exc).__name__
                    log_structured(
                        provider=job.provider,
                        dataset=job.dataset,
                        symbol=job.symbol,
                        stage=f"http_retry_reason:{reason}",
                        attempt=att_no,
                    )
                    raise
    raise FetchError("Fetch failed after all retry attempts")
