# ADR-0001: Use Polars over pandas

Date: 2026-03-26  
Status: Accepted

## Context

VertexLab processes table-like financial data in performance-sensitive pipelines. The project needed predictable, typed, and efficient dataframe operations for validation, transformation, and writer integration.

## Decision

Use Polars as the primary dataframe engine instead of pandas for core pipeline data handling.

## Consequences

- Better performance characteristics for common analytical operations.
- Clearer schema-oriented workflows through Polars dtypes.
- Fewer implicit conversions compared to pandas-heavy flows.
- Team and contributors need Polars familiarity for most data-manipulation changes.
