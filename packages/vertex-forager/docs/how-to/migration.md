# Migration Guide

Use this guide when upgrading between `vertex-forager` releases and checking for behavior changes that may affect your pipelines.

## Upcoming release

### `RetryConfig.enable_http_status_retry` removed

`RetryConfig.enable_http_status_retry` has been removed from the public retry API.
Disable HTTP status retries by passing an empty tuple instead:

```python
from vertex_forager import RetryConfig

retry = RetryConfig(retry_status_codes=())
```

`Retry-After` integer headers are now honored automatically and capped by `max_backoff_s` before falling back to jitter backoff.

**Action required**:

- Remove `enable_http_status_retry` from `RetryConfig(...)` construction.
- Use `retry_status_codes=()` to disable status-based retry.
- Review provider-specific retry expectations if your upstream returns `Retry-After` headers.

### Local state storage moved to SQLite

Checkpoint state and run history now live in a single SQLite database:

```text
~/.cache/vertex-forager/state.db
```

DLQ payloads stay as Arrow IPC files under `~/.cache/vertex-forager/dlq/<table>/`, but each spool file is now tracked in the `dlq_index` table inside `state.db`.

**Action required**:

- If you upgrade while a run is in progress, restart that run from scratch. Existing JSON checkpoint and run-history files are not migrated.
- Use `vertex-forager runs list` and `vertex-forager dlq list` to inspect local operational state.
- Use `vertex-forager clear --checkpoints`, `--runs`, `--dlq`, or `--all` for selective cleanup.

### `persist_run_history` removed

`persist_run_history` has been removed from `create_client(...)`. Run history is always inserted into `state.db`.

**Action required**:

- Remove `persist_run_history` from `create_client(...)` calls.
- Replace it with retention controls when needed:
  - `checkpoint_retention_days`
  - `run_history_retention_days`

## 0.3.x → 0.4.x

### `RequestSpec.idempotent` flag

`RequestSpec` now includes an `idempotent` field (default `True`).
When set to `False`, the retry controller performs a single attempt regardless of retry configuration.

**Action required**: None for most users. If you build custom `RequestSpec` objects for non-idempotent requests, set `idempotent=False` to prevent duplicate side effects.

```python
from vertex_forager.core.config import RequestSpec

spec = RequestSpec(
    url="https://api.example.com/submit",
    method="POST",
    json_body={"x": 1},
    idempotent=False,
)
```

## 0.2.x → 0.3.x

### Opt-in strict schema validation

`SchemaMapper` now accepts `strict_validation=True`. When enabled, missing required columns or type mismatches raise `ValueError` immediately instead of being silently ignored.

**Action required**: None — strict validation is opt-in. To enable:

```python
from vertex_forager.schema.mapper import SchemaMapper

mapper = SchemaMapper(strict_validation=True)
```

!!! warning
  Enabling strict validation on existing pipelines may surface previously silent schema issues. Test with your data before deploying.

### In-memory writer dedup/upsert

`InMemoryBufferWriter` now supports `unique_key` for automatic deduplication and upsert behavior during writes.

**Action required**: None — the feature is additive.

## 0.1.x → 0.2.x

!!! danger "Breaking changes"
  This is a major restructuring release. Most imports changed.

### Public API moved to package root

All primary classes and factories are now importable directly from `vertex_forager`:

```python
# Before (0.1.x)
from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import RetryConfig

# After (0.2.x)
from vertex_forager import BaseClient, RetryConfig, create_client
```

### Centralized constants

Provider-specific constants moved to `vertex_forager.constants`.

### Environment-aware CLI

The CLI now reads environment variables and supports `vertex-forager collect`, `vertex-forager status`, and `vertex-forager recover`.

### DLQ spool

Dead Letter Queue spooling is always enabled; failed write packets are persisted to disk with `fsync` and atomic replace.

### Writer chunked flush

Writers use internal chunked flush to bound memory usage during large writes.

### RunResult summaries

Pipeline runs return `RunResult` with per-table DLQ counts.

### Logging model

OBS/stage logs are now always emitted at DEBUG level with structured `extra` fields on the `vertex_forager` logger. Configure visibility in the host app:

```python
import logging

logging.getLogger("vertex_forager").setLevel(logging.DEBUG)
```

Removed parameters:

- `structured_logs`
- `log_verbose`

### Tracing model

Tracing now follows the OpenTelemetry library standard. The library acquires its own tracer via `trace.get_tracer("vertex_forager")` and emits spans unconditionally. The host application controls whether spans are exported by configuring a `TracerProvider`:

> **Note**: The snippet below uses `TracerProvider`, `BatchSpanProcessor`, and `OTLPSpanExporter` from the OpenTelemetry SDK and OTLP exporter packages. These are **not included** in the vertex-forager runtime dependency — install them separately:

> ```
> pip install opentelemetry-sdk opentelemetry-exporter-otlp
> ```

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)
```

When no `TracerProvider` is configured, the OTel SDK falls back to a no-recording tracer — spans are not recorded or exported.

Removed parameters:

- `otel_enabled`
- `tracer`
- `advanced` / `AdvancedConfig`

**Action required**: Update imports to use the package root and review any code that parses `RunResult`.

## 0.1.0

Initial release. No migration needed.
