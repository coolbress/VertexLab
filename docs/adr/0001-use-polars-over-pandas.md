# ADR-0001: Use Polars over pandas

Date: 2026-03-26  
Status: Accepted

## Context

VertexLab processes table-like financial data in performance-sensitive pipelines. The project needed predictable, typed, and efficient dataframe operations for validation, transformation, and writer integration.

## Decision

Use Polars as the required dataframe engine for the core pipeline.

Scope definition:

- Core pipeline means data transformation, enrichment, validation, and storage layers.
- Provider/ingress components may temporarily accept pandas DataFrames during transition.
- Before data crosses into core pipeline boundaries, ingress owners must convert pandas payloads to Polars.

Migration policy:

- Transition window: until 2026-06-30 for existing provider/ingress paths.
- Ownership: provider/ingress maintainers own pandas-to-Polars conversion before handoff.
- Allowed exceptions: third-party integrations that only expose pandas may keep pandas internally, but handoff to core pipeline must still be Polars.

## Consequences

- Better performance characteristics for common analytical operations.
- Clearer schema-oriented workflows through Polars dtypes.
- Clear core boundary where Polars is mandatory and pandas interop is limited to ingress.
- Team and contributors need Polars familiarity for most data-manipulation changes.
- Ingress teams may carry temporary conversion overhead during the transition period.
