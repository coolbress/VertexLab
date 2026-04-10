# Schema Reference

This page belongs in the reference section because it documents the exact schema contract objects and lookup API, not the broader design rationale.

Use this page when you want to inspect the schema system itself:

- `TableSchema` field meanings
- `DatasetSpec` field meanings
- `SchemaMapper` API surface
- registry lookup behavior

Use [Providers reference](providers.md) when you want the catalog of built-in datasets and tables.

## What a schema defines

Each output table is described by a `TableSchema` contract.

That contract includes:

- canonical table name
- canonical column-to-dtype mapping
- `unique_key`
- optional `analysis_date_col`
- optional `quality_rules`

This is the structural contract that normalization and persistence rely on.

## Registry lookup

```python
from vertex_forager.schema.registry import get_table_schema

schema = get_table_schema("yfinance_price")
if schema is not None:
    print(schema.table)
    print(schema.unique_key)
    print(schema.analysis_date_col)
```

The shared registry resolves a table name to its canonical `TableSchema`.

## Relationship to other docs

- [Providers reference](providers.md) tells you which provider datasets exist and which tables they target.
- [How schema normalization works](../explanation/how-schema-normalization-works.md) explains why the schema contract matters.
- This page focuses only on the exact API and object model.

## TableSchema

::: vertex_forager.schema.config.TableSchema

## DatasetSpec

::: vertex_forager.schema.config.DatasetSpec

## SchemaMapper

`SchemaMapper` applies the canonical schema contract to parsed frames before writing.

::: vertex_forager.schema.mapper.SchemaMapper

## Registry

::: vertex_forager.schema.registry.get_table_schema
