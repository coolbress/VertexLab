"""Error types and utilities for structured error handling."""

from dataclasses import dataclass


@dataclass(frozen=True)
class RunError:
    """Structured error information for pipeline runs.

    Attributes:
        provider: Data provider name (e.g., "sharadar", "yfinance")
        dataset: Dataset name (e.g., "sep", "actions")
        symbol: Symbol that failed (e.g., "AAPL", "MSFT")
        exc_type: Exception type as string (e.g., "httpx.HTTPStatusError")
        message: Error message
        retryable: Whether this error is retryable (True for 429/503/network errors)
    """

    provider: str
    dataset: str
    symbol: str
    exc_type: str
    message: str
    retryable: bool

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        provider: str,
        dataset: str,
        symbol: str | None,
    ) -> "RunError":
        """Create a RunError from an exception.

        Args:
            exc: The exception that occurred
            provider: Data provider name
            dataset: Dataset name
            symbol: Symbol that failed

        Returns:
            RunError: Structured error information
        """
        exc_type = type(exc).__name__
        message = str(exc)

        # Determine if error is retryable
        retryable = cls._is_retryable_error(exc)

        return cls(
            provider=provider,
            dataset=dataset,
            symbol=symbol or "",
            exc_type=exc_type,
            message=message,
            retryable=retryable,
        )

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        """Determine if an error is retryable.

        Retryable errors include:
        - HTTP 429 (Too Many Requests)
        - HTTP 503 (Service Unavailable)
        - Network errors
        - Timeout errors

        Non-retryable errors include:
        - HTTP 4xx client errors (except 429)
        - Validation errors
        - Schema errors

        Args:
            exc: The exception to check

        Returns:
            bool: True if the error is retryable
        """
        # Check for HTTP status errors
        if hasattr(exc, 'response') and exc.response is not None and hasattr(exc.response, 'status_code'):
            status_code = exc.response.status_code
            # 429 (Too Many Requests) and 503 (Service Unavailable) are retryable
            if status_code in {429, 503}:
                return True
            # 4xx client errors (except 429) are not retryable
            if 400 <= status_code < 500:
                return False

        # Check for network-related errors
        error_type = type(exc).__name__
        retryable_error_types = {
            'TimeoutError',
            'ConnectError',
            'ConnectTimeout',
            'ReadTimeout',
            'PoolTimeout',
            'NetworkError',
        }

        if error_type in retryable_error_types:
            return True

        # Non-retryable by default
        return False
