# ADR-0002: Use run_sync_compat instead of @jupyter_safe

Date: 2026-03-26  
Status: Accepted

## Context

The project needed a consistent sync entry path for environments that may already run an event loop, including notebook contexts. Decorator-based wrapping added indirection and made behavior harder to reason about across call sites.

## Decision

`run_sync_compat` is the official event-loop compatibility boundary for synchronous invocation.

`jupyter_safe` is no longer an independent compatibility layer. It is a wrapper that delegates compatibility behavior by calling `run_sync_compat` internally.

## Consequences

- Event-loop compatibility logic is centralized and explicit.
- Compatibility guarantees, maintenance responsibility, and behavior regression testing are owned at `run_sync_compat`.
- `jupyter_safe` remains as a convenience wrapper surface, but must preserve delegation-only semantics to avoid duplicated compatibility logic.
- Existing and future sync surfaces should route through `run_sync_compat` directly or through wrappers that delegate to it.
