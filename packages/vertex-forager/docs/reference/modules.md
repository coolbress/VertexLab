# Module Index

Overview of the `vertex_forager` package structure. See [API Reference](api.md) for detailed class and function documentation.

## Package Layout

```
vertex_forager/
├── api.py              # Factories (create_client, create_router) and base classes
├── constants.py        # Provider datasets, rate limits, default values
├── exceptions.py       # Exception hierarchy (VertexForagerError → leaf errors)
├── utils.py            # Env helpers, validation, progress bar utilities
├── core/
│   ├── config.py       # EngineConfig, RetryConfig, RequestSpec, FetchJob, FramePacket, RunResult
│   ├── controller.py   # FlowController, GradientConcurrencyLimiter, GCRARateLimiter
│   ├── http.py         # HttpExecutor (async httpx wrapper)
│   ├── pipeline.py     # VertexForager (main async pipeline engine)
│   ├── retry.py        # Tenacity retry controller factory
│   ├── contracts.py    # Protocol types (TracerProtocol, etc.)
│   └── types.py        # TypedDicts and type aliases (JSONValue, DLQStatus, etc.)
├── clients/
│   ├── base.py         # BaseClient (provider-agnostic sync/async interface)
│   ├── sharadar.py     # SharadarClient
│   └── yfinance.py     # YFinanceClient
├── routers/
│   ├── base.py         # BaseRouter (job generation and response parsing)
│   ├── sharadar.py     # SharadarRouter
│   └── yfinance.py     # YFinanceRouter
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
| Configuration | `from vertex_forager import EngineConfig` | [API Reference → Configuration](api.md#configuration) |
| Flow control | `from vertex_forager.core.controller import FlowController` | [API Reference → Flow Control](api.md#flow-control) |
| Writers | `from vertex_forager.writers import create_writer` | [API Reference → Writers](api.md#writers) |
| Exceptions | `from vertex_forager import FetchError, WriterError` | [API Reference → Exceptions](api.md#exceptions) |
