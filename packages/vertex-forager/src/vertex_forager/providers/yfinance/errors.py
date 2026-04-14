"""YFinance-specific error helpers."""

_REQUESTS_RETRYABLE_NAMES = frozenset(
    {
        "ConnectionError",
        "Timeout",
        "ConnectTimeout",
        "ReadTimeout",
    }
)


def is_retryable_yfinance_error(exc: Exception) -> bool:
    """Return True for retryable transport errors surfaced by yfinance."""
    exc_type = type(exc)
    return exc_type.__module__ == "requests.exceptions" and exc_type.__name__ in _REQUESTS_RETRYABLE_NAMES


__all__ = ["is_retryable_yfinance_error"]
