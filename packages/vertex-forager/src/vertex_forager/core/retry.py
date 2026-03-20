"""Retry strategies using tenacity."""

from __future__ import annotations

import logging
import secrets
from typing import TYPE_CHECKING

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
)

if TYPE_CHECKING:
    from vertex_forager.core.config import RetryConfig

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
        if config.enable_http_status_retry and isinstance(exc, httpx.HTTPStatusError):
            resp = getattr(exc, "response", None)
            status = getattr(resp, "status_code", None)
            if status in set(config.retry_status_codes):
                return True
        return False

    def _wait_capped(retry_state: RetryCallState) -> float:
        att = retry_state.attempt_number
        expo = config.base_backoff_s * (2 ** max(0, att - 1))
        cap = max(0.0, min(config.max_backoff_s, expo))
        if cap <= 0.0:
            return 0.0
        rnd = secrets.SystemRandom()
        return float(rnd.uniform(0.0, cap))

    # For non-idempotent requests, guard against repeats by forcing a single attempt.
    stop_policy = stop_after_attempt(1 if not idempotent else config.max_attempts)

    return AsyncRetrying(
        stop=stop_policy,
        wait=_wait_capped,
        retry=retry_if_exception(_should_retry),
        before_sleep=before_sleep_log(logger, log_level),
        reraise=True,
    )
