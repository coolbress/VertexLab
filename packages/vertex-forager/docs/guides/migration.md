# Migration Guide

This guide covers breaking changes and upgrade steps between vertex-forager releases.

## 0.3.x → 0.4.x

**Release date**: 2026-03-20

### `RequestSpec.idempotent` flag

`RequestSpec` now includes an `idempotent` field (default `True`).
When set to `False`, the retry controller performs a **single attempt** regardless of retry configuration.

**Action required**: None for most users. If you build custom `RequestSpec` objects for non-idempotent requests (e.g., `POST` without idempotency keys), set `idempotent=False` to prevent duplicate side effects.

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

**Release date**: 2026-03-20

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

**Release date**: 2026-03-16

!!! danger "Breaking changes"
  This is a major restructuring release. Most imports changed.

### Public API moved to package root

All primary classes and factories are now importable directly from `vertex_forager`:

```python
# Before (0.1.x)
from vertex_forager.clients.base import BaseClient
from vertex_forager.core.config import EngineConfig

# After (0.2.x)
from vertex_forager import BaseClient, EngineConfig, create_client
```

### Centralized constants

Provider-specific constants (datasets, rate limits) moved to `vertex_forager.constants`.

### Environment-aware CLI

The CLI now reads environment variables (`VF_PROVIDER`, `VF_TICKERS`, `SHARADAR_API_KEY`) and supports `vertex-forager collect`, `vertex-forager status`, and `vertex-forager recover` commands.

### DLQ spool

Dead Letter Queue spooling is now enabled by default. Failed write packets are persisted to disk with `fsync` and atomic replace. Disable with `EngineConfig(dlq_enabled=False)`.

### Writer chunked flush

Writers now support chunked flush via `EngineConfig(writer_chunk_rows=...)` to bound memory peak during large writes.

### RunResult summaries

Pipeline runs now return `RunResult` with per-table DLQ counts, regardless of `metrics_enabled` setting.

**Action required**: Update all imports to use the package root. Review `RunResult` output format if you parse pipeline results programmatically.

## 0.1.0

**Release date**: 2026-03-15

Initial release. No migration needed.

- Pipeline orchestration with async HTTP executor
- Sharadar and YFinance providers
- DuckDB writer with unique index support
- CLI interface
