# ADR-0002: Use run_sync_compat instead of @jupyter_safe

Date: 2026-03-26  
Status: Accepted

## Context

The project needed a consistent sync entry path for environments that may already run an event loop, including notebook contexts. Decorator-based wrapping added indirection and made behavior harder to reason about across call sites.

## Decision

Use `run_sync_compat` as the explicit compatibility boundary for sync invocation instead of relying on the `@jupyter_safe` decorator pattern.

## Consequences

- Event-loop compatibility logic is centralized and explicit.
- Call paths are easier to audit and test than scattered decorator usage.
- Existing and future sync surfaces should consistently route through the helper.
- Maintainers must keep helper semantics stable because it is a key execution boundary.
