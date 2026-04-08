# API Reference

## Factories

Start here when you want the stable default entry points without importing lower-level implementation classes directly.

::: vertex_forager.api.create_client

## Typed clients

These concrete client classes are the typed return values of `create_client(...)` overloads and are useful for type annotations in user code.

```python
from vertex_forager import SharadarClient

def process(client: SharadarClient) -> None:
    ...
```

- `SharadarClient`
  - returned by `create_client(provider="sharadar", ...)`
- `YFinanceClient`
  - returned by `create_client(provider="yfinance", ...)`

Use these names from the package root for annotations and editor assistance, while still preferring `create_client(...)` to construct clients.

## State

These APIs expose credential-free access to persisted local runtime state.

::: vertex_forager.api.StateManager

::: vertex_forager.api.DLQEntry

::: vertex_forager.api.ReplayResult

::: vertex_forager.api.RunRecord

## Pipeline Results

These result models summarize user-visible run outcomes.

::: vertex_forager.api.RunResult

::: vertex_forager.api.ProgressSnapshot

## Configuration

Configuration models are documented in the dedicated [Configuration reference](config.md).

## Exceptions

These are the main public exception types you are most likely to catch around client usage and local-state operations.

Hierarchy:

- `VertexForagerError`
  - `FetchError`
  - `WriterError`
  - `DataQualityError`
  - `ValidationError`
  - `CheckpointNotFoundError`
  - `InputError`

Catch `VertexForagerError` when you want one broad package-level failure boundary. Catch the more specific subclasses below when you want different recovery behavior.

### `VertexForagerError`

Base exception for public vertex-forager failures.

::: vertex_forager.api.VertexForagerError

### `InputError`

Raised when caller-provided parameters are invalid before the run can proceed.

::: vertex_forager.api.InputError

### `FetchError`

Raised when provider fetch or transport work fails in a user-visible way.

::: vertex_forager.api.FetchError

### `WriterError`

Raised when persistence fails at the write boundary.

::: vertex_forager.api.WriterError

### `ValidationError`

Raised when normalized data does not satisfy required validation rules.

::: vertex_forager.api.ValidationError

### `DataQualityError`

Raised when `quality_check="error"` converts a quality violation into a hard failure.

::: vertex_forager.api.DataQualityError

### `CheckpointNotFoundError`

Raised when checkpoint resume is requested for a table that has no resumable checkpoint.

::: vertex_forager.api.CheckpointNotFoundError

Lower-level exception subclasses still exist in `vertex_forager.exceptions`, but they are not part of the primary end-user surface documented here.

## Lower-level implementation modules

Pipeline engine internals, flow-control helpers, HTTP/retry executors, writer implementations, lifecycle helpers, and convenience utilities still exist in implementation modules, but they are no longer part of the primary user-facing import surface documented here.

`BaseRouter` and `create_router` are unstable extension points and are not part of the public, semver-guaranteed API. Prefer the documented public entrypoints such as `create_client(...)` and `StateManager()`.

## Thread Safety

### `create_client()` and client instances

- `create_client()` itself is safe to call from any ordinary single-threaded setup path.
- The resulting client instance should be treated as a single-owner runtime object.
- Recommended usage is one client instance per thread, task boundary, or process rather than sharing one client across multiple threads concurrently.

### `StateManager()`

- `StateManager()` is safe for normal concurrent use when each thread or process creates its own instance.
- Its operations use short-lived SQLite connections, so concurrent readers and independent writers can coordinate through SQLite locking.
- Recommended usage is separate `StateManager()` instances per thread or process, not one shared object used as a synchronization primitive.
