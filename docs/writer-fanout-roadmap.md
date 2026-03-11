# Writer Fan-out Roadmap: Table Sharding or External OLAP (Future)

## Context
- Current pipeline uses a single writer instance (e.g., DuckDBWriter) that upserts normalized frames with PK-based uniqueness and schema evolution.
- DuckDB offers excellent single-node performance but does not support multi-writer concurrency to the same database file safely.
- Growth scenarios require higher ingest throughput and isolation across tables/domains, motivating fan-out or migration to managed OLAP.

Related code references:
- Writer base: [writers/base.py](packages/vertex-forager/src/vertex_forager/writers/base.py)
- DuckDB writer: [writers/duckdb.py](packages/vertex-forager/src/vertex_forager/writers/duckdb.py)
- Pipeline write path: [core/pipeline.py](packages/vertex-forager/src/vertex_forager/core/pipeline.py)

## Preconditions
- Do not open the same DuckDB file from multiple writers concurrently.
- Consider external OLAP if multi-writer durability/scale is required beyond single-node DuckDB.
- Maintain strict PK-based idempotency and error isolation (DLQ) semantics.

## Design Goals
- Remove the single-writer bottleneck when throughput increases.
- Preserve atomicity/consistency guarantees and provide clear error isolation and reprocessing.
- Integrate with existing backpressure/rate limiting (FlowController) and metrics hooks.

## Option A: Table-Level Fan-out (Multiple Sinks)
### Overview
- Partition by table (and optionally by domain or date-range), route each partition to its own destination (e.g., separate DuckDB files).
- A single orchestrator process owns routing and ensures no two writers touch the same file concurrently.

### Architecture
- Add a fan-out dispatcher (in pipeline or writer facade) that maps `FramePacket.table` → destination URI.
- Instantiate one writer per destination (e.g., `duckdb://./dbs/sharadar_sep.duckdb`, `duckdb://./dbs/yfinance_price.duckdb`).
- Route writes by table; maintain per-writer async locks to prevent overlapping transactions.

### Transactionality & Ordering
- Ordering within a table is preserved by routing; writers use upsert with unique index (PK).
- For strict ordering across domains, retain current queue priority (pagination-first) and flush strategy.

### Error Isolation & Reprocessing
- DLQ remains per table; failures in one destination do not block others.
- Recovery uses existing `vertex-forager recover` flow to reinject IPC batches to a chosen destination.

### Backpressure & Resource Management
- Limit concurrent writer flushes per destination.
- Reuse existing metrics hooks to emit per-destination write latency p95/p99 and rows written.
- Use environment tunables for HTTP concurrency independently of writer concurrency.

### Pros/Cons
- Pros: Minimal change to storage engine; straightforward routing; easy rollback.
- Cons: More files to manage; single-node limits still apply; backups/compaction must run per file.

## Option B: External OLAP (MotherDuck, ClickHouse)
### Overview
- Migrate hot tables (or all tables) to an OLAP store that supports concurrent writers, columnar compression, and distributed compute.

### Architecture
- Introduce new writer(s): `MotherDuckWriter`, `ClickHouseWriter`, with shared `BaseWriter` interface.
- Configuration maps table → destination URI (e.g., `md://db.schema/table`, `ch://host:port/db/table`).
- Preserve `write` and `write_bulk` semantics; implement upsert via:
  - MotherDuck: DuckDB-compatible SQL with `ON CONFLICT` or MERGE patterns if supported.
  - ClickHouse: INSERT with ReplacingMergeTree; deduplication keyed by PK + version or UUID.

### Transactionality & Ordering
- Idempotency enforced via PK (and possibly version column); eventual consistency accepted for CH merge engines.
- Ordering across shards is not required if consumer queries rely on PK/analysis_date ordering.

### Error Isolation & Reprocessing
- Keep DLQ in IPC form for failed batches; add per-destination DLQ routing.
- Provide a simple “reinject to OLAP” CLI path analogous to DuckDB recover.

### Backpressure & Resource Management
- Writers manage connection pools (MotherDuck over DuckDB/HTTP, ClickHouse native protocol/HTTP).
- Expose max connections/keepalive where applicable; reuse existing `default_async_client` env tunables for HTTP flows.

### Pros/Cons
- Pros: Horizontal scalability, managed durability, better analytical performance for large datasets.
- Cons: New dependencies and credentials; upsert/idempotency logic varies by engine; migration complexity.

## Migration Path
1. Stage 0 (Current): Single DuckDBWriter per run; PK indexes; DLQ isolation.
2. Stage 1 (Fan-out Local): Table → separate DuckDB files; dispatcher ensures single-writer per file.
3. Stage 2 (Hybrid): Move hot tables to MotherDuck/ClickHouse; cold tables remain on DuckDB; dual writers via routing.
   - Hot-table selection criteria:
     - High write volume (rows/s) or frequent corrections requiring upsert pressure
     - Critical-path analytics depend on low query latency or near real-time availability
     - Sustained compaction overhead (VACUUM/CHECKPOINT) or WAL growth
     - Storage pressure or backup windows exceeding acceptable thresholds
   - Benchmark template:
     - Workload: dataset list, symbol count, row size distribution, time range
     - Metrics:
       - Ingest throughput (rows/s), writer flush latency p95/p99
       - OLAP insert/merge latency p95/p99, dedup effectiveness
       - Query latency p95/p99 for top N analytical queries
       - CPU/Mem/IO utilization during ingest and query
       - Compaction/merge overhead (time, IO)
     - Method:
       - Run A/B on DuckDB vs OLAP with identical inputs
       - Repeat N runs and report median and dispersion
4. Stage 3 (Full OLAP): All tables reside in OLAP; DuckDB used for local dev/testing or as cache.

## Configuration Sketch
```yaml
writers:
  default: "duckdb://./forager.duckdb"
  routes:
    sharadar_sep: "duckdb://./dbs/sharadar_sep.duckdb"
    yfinance_price: "md://forager.public/yfinance_price"
    yfinance_news: "ch://ch.prod:8443/forager/yfinance_news"
limits:
  writer_concurrency: 2        # per destination
  http:
    max_connections: ${VF_HTTP_MAX_CONNECTIONS}
    max_keepalive: ${VF_HTTP_MAX_KEEPALIVE}
```

## Observability & Operations
- Metrics: per-destination counters, flush latency p95/p99, rows written.
- Logs: structured events for `write_flush`, `dlq_spooled`, `dlq_rescued_*`.
- Maintenance: periodic VACUUM/CHECKPOINT for DuckDB; retention policies for DLQ; connection health checks for OLAP.

## Configuration Examples
- MotherDuck DSN
  - `md://database.schema/table`
  - Auth: environment variable `MOTHERDUCK_TOKEN` loaded by writer; avoid printing tokens in logs.
  - Network: HTTPS to MotherDuck endpoint; respect corporate proxies if present.
- ClickHouse DSN
  - `ch://user:password@host:8443/database/table?secure=true&compression=lz4`
  - Engine: ReplacingMergeTree with version column or dedup key
  - Network: TLS required in production; allowlist egress; tune max_concurrent_inserts and max_partitions_per_insert
- Routing sketch
  - `writers.routes.yfinance_news: ch://ch.prod:8443/forager/yfinance_news`
  - `writers.routes.yfinance_price: md://forager.public/yfinance_price`

## Security
- Credentials loaded via environment or secrets manager; never logged.
- Rotate keys via client/writer lifecycle hooks (similar to `before_http_request` in HttpExecutor).

## Risks & Trade-offs
- Multi-destination complexity in configuration and monitoring.
- Upsert semantics differ across OLAP engines; carefully define idempotency.
- Operational overhead for backups and schema evolution across multiple stores.

## Acceptance Criteria
- This document captures trade-offs, architecture options, and a phased migration plan.
- No code changes are required to maintain current single-writer correctness.
