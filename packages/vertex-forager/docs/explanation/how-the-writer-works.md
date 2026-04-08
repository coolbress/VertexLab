# How the Writer Works

The writer is the boundary between normalized table-shaped data and durable local storage.

## What the writer is responsible for

The writer handles:

- creating tables when needed
- evolving schemas when new columns are introduced
- validating primary-key requirements
- applying upsert semantics
- isolating failed packets for DLQ handling

## Upsert behavior

DuckDB writes are keyed by the table schema’s `unique_key`.

If a row conflicts on that logical key:

- the existing row is updated

If there is no conflict:

- the row is inserted normally

This is what makes persisted reruns idempotent instead of duplicating rows.

## Schema evolution

The writer does not assume the target table is frozen forever. When the canonical schema gains new columns, the writer can add the missing columns before writing new data.

That lets the storage contract evolve without forcing users to rebuild every DuckDB file from scratch.

## Identifier safety

The writer validates and quotes table and column identifiers before using them in DuckDB statements.

This protects the persistence path from malformed or unsafe identifiers while still allowing ordinary SQL reserved words to work through quoted identifiers.

## Value handling

Values are not interpolated into SQL strings by hand. The writer uses registered views and structured write paths instead of building insert statements with raw values.

## Thread and async model

The DuckDB writer uses a single-writer async lock and a single-threaded executor for the underlying sync DuckDB work. That keeps write execution serialized even though the pipeline around it is asynchronous.

## Failure path

If a merged flush fails:

- the pipeline tries packet-level rescue writes
- packets that still fail are spooled to DLQ

So the writer is closely tied to the package’s persistence and recovery story, not just to ordinary successful inserts.

## Related pages

- [How DLQ works](how-dlq-works.md)
- [How schema normalization works](how-schema-normalization-works.md)
- [How pipeline orchestrates](how-pipeline-orchestrates.md)
