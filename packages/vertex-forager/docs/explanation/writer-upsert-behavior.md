# Writer Upsert Behavior (DuckDB)

## Overview
- Writer performs transactional upserts so re-fetches update existing records rather than duplicating them.
- Upsert keys are driven by the table schema's `unique_key` in the registry.

## Prerequisites
- Ensure the target table defines `unique_key` in the schema registry.
- Example for Sharadar price:
  - `unique_key=("provider", "ticker", "date")`

## Upsert Logic
- When writing a packet:
  - If the row conflicts on `unique_key`, perform "insert or replace" (or `ON CONFLICT DO UPDATE`) to update values.
  - If no conflict, perform a standard `INSERT`.
- This guarantees idempotent corrections and backfills.

## Example Scenario
1. Insert `AAPL @ 2024-01-01, close=100.0` → row count 1
2. Insert `AAPL @ 2024-01-01, close=105.0` (same key) → row count 1, `close` updated to 105.0
3. Insert `AAPL @ 2024-01-02, close=110.0` (new key) → row count 2

## Verification
- See verification test under `packages/vertex-forager/tests/verification/verify_duckdb_upsert.py`.
