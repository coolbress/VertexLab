from __future__ import annotations

class VertexForagerError(Exception):
    pass

class InputError(VertexForagerError):
    pass

class FetchError(VertexForagerError):
    pass

class TransformError(VertexForagerError):
    pass

class WriterError(VertexForagerError):
    pass

class ComputeError(VertexForagerError):
    pass

class ValidationError(VertexForagerError):
    pass

class PrimaryKeyMissingError(ValidationError):
    def __init__(self, *, table: str, column: str) -> None:
        self.table = table
        self.column = column
        super().__init__(f"Missing PK column '{column}' in table '{table}'")

class PrimaryKeyNullError(ValidationError):
    def __init__(self, *, table: str, column: str, null_count: int) -> None:
        self.table = table
        self.column = column
        self.null_count = null_count
        super().__init__(f"PK column '{column}' in table '{table}' has {null_count} nulls")
