from __future__ import annotations


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


__all__ = [
    "ComputeError",
    "DLQSpoolError",
    "FetchError",
    "InputError",
    "PrimaryKeyMissingError",
    "PrimaryKeyNullError",
    "TransformError",
    "ValidationError",
    "VertexForagerError",
    "WriterError",
]
