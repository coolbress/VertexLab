# Core Error Policy

## Goals

- Keep core-internal imports stable through `vertex_forager.core.errors`.
- Separate transport/infra failures from domain/data validation failures.
- Ensure pipeline and worker components emit structured `RunError` entries.

## Import Rules

- Core modules should import public error types from `vertex_forager.core.errors`.
- `vertex_forager.exceptions` remains the canonical declaration module.
- `vertex_forager.core.errors` acts as the core-facing compatibility/export layer.

## Categories

- Domain/Data validation:
  - `ValidationError`
  - `PrimaryKeyMissingError`
  - `PrimaryKeyNullError`
- Fetch/transport:
  - `FetchError`
- DLQ fallback:
  - `DLQSpoolError`
- Common base:
  - `VertexForagerError`

## Recording Rules

- User-visible pipeline/reporting errors are stored as `RunError`.
- For writer DLQ fallback paths, preserve original context (`table`, `provider`, `symbol`) and classify by prefix (`WriterError`, `DuckDBError`, `UnexpectedWriterError`).

## Extension Rules

- New exception classes should be declared in `vertex_forager.exceptions`.
- If they are used by core modules, re-export them from `vertex_forager.core.errors` and include them in `__all__`.
