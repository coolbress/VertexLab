from __future__ import annotations

from typing import Any

DEFAULT_RETRY_STATUS_CODES: tuple[int, ...] = (429, 503)


def is_retryable_status_code(
    status_code: Any,
    retry_status_codes: tuple[int, ...] = DEFAULT_RETRY_STATUS_CODES,
) -> bool:
    try:
        normalized = int(status_code)
    except (TypeError, ValueError):
        return False
    return normalized in set(retry_status_codes)
