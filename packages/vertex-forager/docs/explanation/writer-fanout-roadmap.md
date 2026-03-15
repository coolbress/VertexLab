# Writer Fan-out Roadmap: Table Sharding or External OLAP (Future)

## Context

- Current pipeline uses a single writer instance (e.g., DuckDBWriter) that upserts normalized frames with PK-based uniqueness and schema evolution.
- DuckDB offers excellent single-node performance but does not support multi-writer concurrency to the same database file safely.
- Growth scenarios require higher ingest throughput and isolation across tables/domains, motivating fan-out or migration to managed OLAP.

## Preconditions

- Do not open the same DuckDB file from multiple writers concurrently.
- Consider external OLAP if multi-writer durability/scale is required beyond single-node DuckDB.
- Maintain strict PK-based idempotency and error isolation (DLQ) semantics.

## Design Goals

- Remove the single-writer bottleneck when throughput increases.
- Preserve atomicity/consistency guarantees and provide clear error isolation and reprocessing.
- Integrate with existing backpressure/rate limiting (FlowController) and metrics hooks.

## Option A: Table-Level Fan-out (Multiple Sinks)

- Partition by table (and optionally by domain or date-range), route each partition to its own destination (e.g., separate DuckDB files).
- A single orchestrator process owns routing and ensures no two writers touch the same file concurrently.
- Add a dispatcher mapping `FramePacket.table` → destination URI; per-destination async locks prevent overlapping transactions.
- Ordering within a table is preserved by routing; writers use upsert with unique index (PK).
- DLQ remains per table; failures in one destination do not block others.

## Option B: External OLAP (MotherDuck, ClickHouse)

- Introduce new writers with shared `BaseWriter` interface.
- Map table → destination URI (e.g., `md://db.schema/table`, `ch://host:port/db/table`).
- Implement upsert semantics per engine; maintain idempotency.

## Migration Path

1) Current: Single DuckDBWriter  
2) Fan-out Local: Table → separate DuckDB files  
3) Hybrid: Hot tables to OLAP; cold tables remain on DuckDB  
4) Full OLAP

## Observability & Operations

- Emit per‑destination flush latency p95/p99 and rows written.
- Maintain DLQ routing and recovery CLI compatibility.
