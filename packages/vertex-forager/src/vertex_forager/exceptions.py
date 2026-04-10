from __future__ import annotations

from dataclasses import dataclass

try:
    import httpx

    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from vertex_forager.core.retry_policy import DEFAULT_RETRY_STATUS_CODES, is_retryable_status_code


class VertexForagerError(Exception):
    """Base exception for all Vertex Forager errors.

    Raised when an operation within the Vertex Forager system encounters an error
    that does not fit a more specific category.
    """


class InputError(VertexForagerError):
    """Invalid user or configuration input.

    Raised when parameters, identifiers, or configuration values are invalid.
    """


class FetchError(VertexForagerError):
    """Network or external provider fetch failures.

    Raised when HTTP/library calls return errors or unreachable resources.
    """


class TransformError(VertexForagerError):
    """Data transformation/normalization failures.

    Raised during parsing or schema normalization when data cannot be shaped.
    """


class WriterError(VertexForagerError):
    """Persistence/write failures.

    Raised by writers when storage operations fail.
    """


class ComputeError(VertexForagerError):
    """Computation failures during data processing."""


class ValidationError(VertexForagerError):
    """Schema or data validation failures."""


class CheckpointNotFoundError(VertexForagerError):
    """Requested checkpoint state is not available."""


class SchemaMapError(TransformError):
    """Raised when an unsupported schema type mapping is encountered in strict mode."""


class PrimaryKeyMissingError(ValidationError):
    """Primary key column missing from data.

    Indicates that required primary key columns are absent in the dataset being
    processed or written.

    Args:
        table: Target table name.
        column: Missing primary key column name(s).

    Attributes:
        table: Target table name.
        column: Missing primary key column name(s).
    """

    def __init__(self, *, table: str, column: str) -> None:
        self.table = table
        self.column = column
        super().__init__(f"Missing PK column '{column}' in table '{table}'")


class PrimaryKeyNullError(ValidationError):
    """Primary key contains null values.

    Indicates that the primary key column includes one or more nulls which would
    violate uniqueness constraints or upsert logic.

    Args:
        table: Target table name.
        column: Primary key column name.
        null_count: Number of nulls detected.

    Attributes:
        table: Target table name.
        column: Primary key column name.
        null_count: Number of nulls detected.
    """

    def __init__(self, *, table: str, column: str, null_count: int) -> None:
        self.table = table
        self.column = column
        self.null_count = null_count
        super().__init__(f"PK column '{column}' in table '{table}' has {null_count} nulls")


class DLQSpoolError(VertexForagerError):
    """Dead Letter Queue (DLQ) spooling failure.

    Raised when persisting failed packets to the DLQ fails. Carries counts that
    summarize partial rescue progress and remaining items.

    Attributes:
        rescued: Number of items successfully rescued (written) before the spool attempt.
        remaining: Number of items left to persist in the DLQ when the failure occurred.
        original: Optional underlying exception that triggered the spool failure.

    Example:
        raise DLQSpoolError(rescued=1, remaining=3, original=exc)
    """

    def __init__(self, *, rescued: int, remaining: int, original: Exception | None = None) -> None:
        self.rescued = rescued
        self.remaining = remaining
        self.original = original
        msg = f"DLQ spool failed: rescued={rescued} remaining={remaining}"
        if original is not None:
            msg = f"{msg}: {original}"
        super().__init__(msg)


class DataQualityError(VertexForagerError):
    """Raised when data-quality validation fails in strict mode."""

    def __init__(self, *, table: str, rule: str, violations: list[str]) -> None:
        self.table = table
        self.rule = rule
        self.violations = violations
        super().__init__(f"Data quality validation failed for table '{table}' with rule '{rule}'")


@dataclass(frozen=True)
class RunError:
    """Structured error information for pipeline runs."""

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
    ) -> RunError:
        exc_type = f"{exc.__class__.__module__}.{exc.__class__.__name__}"
        message = str(exc)
        retryable = any(cls._is_retryable_error(candidate) for candidate in cls._iter_exception_chain(exc))
        return cls(
            provider=provider,
            dataset=dataset,
            symbol=symbol or "",
            exc_type=exc_type,
            message=message,
            retryable=retryable,
        )

    @staticmethod
    def _iter_exception_chain(exc: Exception) -> list[Exception]:
        chain: list[Exception] = []
        seen: set[int] = set()
        current: Exception | None = exc
        while current is not None and id(current) not in seen:
            chain.append(current)
            seen.add(id(current))
            next_exc = current.__cause__ or current.__context__
            current = next_exc if isinstance(next_exc, Exception) else None
        return chain

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        if hasattr(exc, "response") and exc.response is not None and hasattr(exc.response, "status_code"):
            status_code = exc.response.status_code
            if is_retryable_status_code(status_code, DEFAULT_RETRY_STATUS_CODES):
                return True
            try:
                normalized_status_code = int(status_code)
            except (TypeError, ValueError):
                normalized_status_code = None
            if normalized_status_code is not None and 400 <= normalized_status_code < 500:
                return False
        if isinstance(exc, TimeoutError):
            return True
        if HAS_HTTPX and isinstance(
            exc,
            (
                httpx.TransportError,
                httpx.ConnectError,
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.PoolTimeout,
                httpx.NetworkError,
            ),
        ):
            return True
        return HAS_REQUESTS and isinstance(
            exc,
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
            ),
        )


__all__ = [
    "CheckpointNotFoundError",
    "ComputeError",
    "DLQSpoolError",
    "DataQualityError",
    "FetchError",
    "InputError",
    "PrimaryKeyMissingError",
    "PrimaryKeyNullError",
    "RunError",
    "SchemaMapError",
    "TransformError",
    "ValidationError",
    "VertexForagerError",
    "WriterError",
]
