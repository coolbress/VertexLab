"""Retry strategies using tenacity."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from vertex_forager.core.config import RetryConfig

logger = logging.getLogger("vertex_forager.retry")


def create_retry_controller(
    config: RetryConfig,
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
    return AsyncRetrying(
        stop=stop_after_attempt(config.max_attempts),
        wait=wait_exponential(
            multiplier=config.base_backoff_s,
            max=config.max_backoff_s,
            exp_base=2,
        ),
        retry=retry_if_exception_type(retry_on),
        before_sleep=before_sleep_log(logger, log_level),
        reraise=True,
    )
