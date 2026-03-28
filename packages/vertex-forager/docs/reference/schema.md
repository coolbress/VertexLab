# Schema

vertex-forager uses `TableSchema` objects to describe the canonical shape of each output table. Schemas define the target columns, types, unique keys, and optional data-quality rules that the mapper and writers rely on during normalization and persistence.

Use the schema reference when you want to:

- understand the columns and keys expected for a provider table
- configure strict validation with `SchemaMapper`
- inspect registry lookup behavior before writing custom integrations

## Usage example

```python
from vertex_forager.schema.registry import get_table_schema

schema = get_table_schema("yfinance_price")
if schema is not None:
    print(schema.unique_key)
    print(schema.analysis_date_col)
```

The registry function looks up a table name in the shared provider registry and returns the matching `TableSchema` if one exists. Providers such as Sharadar and yfinance register their schemas centrally so downstream normalization and writers can stay provider-agnostic.

## TableSchema

::: vertex_forager.schema.config.TableSchema

## SchemaMapper

::: vertex_forager.schema.mapper.SchemaMapper

## Registry

::: vertex_forager.schema.registry.get_table_schema
