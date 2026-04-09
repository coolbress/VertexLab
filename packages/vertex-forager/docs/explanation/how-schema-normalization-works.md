# How Schema Normalization Works

Schema normalization is the step that turns provider-shaped payloads into vertex-forager’s internal storage contract.

## `TableSchema` defines the storage contract

`TableSchema` describes:

- canonical table name
- canonical column names
- target Polars dtypes
- `unique_key`
- optional `analysis_date_col`
- optional quality rules

This is the internal contract the writer and quality system depend on.

## `DatasetSpec` maps vendor requests to storage

`DatasetSpec` ties a fetchable dataset to:

- its `TableSchema`
- the provider endpoint or method alias
- the request-side date filter column

This is how one provider-facing dataset resolves into one stable internal table contract.

## `SchemaMapper` enforces the contract

After parsing, `SchemaMapper` normalizes the frame by:

- checking that required columns exist or can be filled
- casting columns into the target schema
- ordering by `analysis_date_col` when applicable
- preserving extra columns only when the schema is marked flexible

## Why the DuckDB table shape stays stable

The same collect method should always target the same internal table contract.

That means:

- missing provider columns become nulls when the schema expects them
- columns the provider never returns do not appear magically
- partial responses still normalize toward the same canonical table layout

## Strict versus non-strict behavior

In strict mode, unsupported or invalid casts fail loudly.

In non-strict mode, the mapper can coerce more aggressively to keep the pipeline moving when the payload is close enough to the expected contract.

If no schema is registered for a table, the mapper emits a warning and passes the frame through unchanged.

If schema casting still triggers a Polars-level failure, the mapper keeps the original frame instead of raising.

## Why this matters to users

If your local DuckDB table has a specific set of columns, that shape comes from `TableSchema`, not from whichever columns happened to appear in one API response.

## Related pages

- [How data quality rules work](how-data-quality-rules-work.md)
- [How the writer works](how-the-writer-works.md)
- [Schema reference](../reference/schema.md)
