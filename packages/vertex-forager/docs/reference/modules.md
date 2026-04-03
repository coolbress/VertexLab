# Module Index

Overview of the `vertex_forager` package structure. See [API Reference](api.md) for detailed class and function documentation.

## Package Layout

```text
vertex_forager/
├── api.py              # Public API helpers (client/router/writer factories)
├── cli.py              # CLI entrypoint
├── constants.py        # Provider datasets, rate limits, default values
├── exceptions.py       # Exception hierarchy (VertexForagerError → leaf errors)
├── utils.py            # App-root helpers, validation, progress bar utilities
├── core/
│   ├── pipeline.py     # VertexForager (main async pipeline engine)
│   ├── lifecycle.py    # Run setup/finalize lifecycle helpers
│   ├── scheduler.py    # DRR pagination fairness helpers and per-symbol queues
│   ├── workerio.py     # Worker fetch/parse/emit helpers
│   ├── writerflush.py  # Writer buffering/flush/error handling
│   ├── retry.py        # Retry policy + runtime retry executor
│   ├── quality.py      # Data quality rules + validation runtime
│   ├── dlq.py          # DLQ spool/recovery helpers
│   ├── errors.py       # Core-facing error exports + RunError
│   ├── config.py       # Public config models, runtime snapshots, RequestSpec, FetchJob, FramePacket, RunResult
│   ├── controller.py   # FlowController, GradientConcurrencyLimiter, GCRARateLimiter
│   ├── http.py         # HttpExecutor (async httpx wrapper)
│   ├── contracts.py    # Protocol types (TracerProtocol, etc.)
│   └── types.py        # TypedDicts and type aliases (JSONValue, DLQStatus, etc.)
├── clients/
│   ├── base.py         # Base client contract/helpers
│   ├── dispatcher.py   # Provider client dispatch
│   └── validation.py   # Client argument/config validation
├── providers/
│   ├── sharadar/       # Sharadar provider implementation (client/router/schema)
│   └── yfinance/       # YFinance provider implementation (client/router/schema)
├── routers/
│   ├── base.py         # Router base protocols/helpers
│   ├── errors.py       # Router-level exceptions
│   ├── jobs.py         # Router job utilities
│   └── transforms.py   # Router transform helpers
├── writers/
│   ├── base.py         # BaseWriter, WriteResult
│   ├── duckdb.py       # DuckDBWriter (async Polars → DuckDB)
│   └── memory.py       # InMemoryBufferWriter
└── schema/
    ├── config.py       # TableSchema definition
    ├── mapper.py       # SchemaMapper (normalization, validation)
    └── registry.py     # Schema registry (get_table_schema)
```

## Key Entry Points

| Use case | Import | Details |
|----------|--------|---------|
| Create a client | `from vertex_forager import create_client` | [API Reference → Factories](api.md#factories) |
| Pipeline engine | `from vertex_forager.core import VertexForager` | [API Reference → Pipeline Engine](api.md#pipeline-engine) |
| Configuration | `from vertex_forager import RetryConfig, AdaptiveThrottleConfig, HTTPConfig` | [API Reference → Configuration](api.md#configuration) |
| Flow control | `from vertex_forager.core.controller import FlowController` | [API Reference → Flow Control](api.md#flow-control) |
| Retry runtime | `from vertex_forager.core.retry import RetryExecutor` | [API Reference → Retry](api.md#retry) |
| Lifecycle helpers | `from vertex_forager.core.lifecycle import RunFinalizer` | [API Reference → Lifecycle](api.md#lifecycle) |
| Writers | `from vertex_forager.writers import create_writer` | [API Reference → Writers](api.md#writers) |
| Core structured error | `from vertex_forager.core.errors import RunError` | [API Reference → Core Errors](api.md#core-errors) |
| Exceptions | `from vertex_forager import FetchError, WriterError` | [API Reference → Exceptions](api.md#exceptions) |
