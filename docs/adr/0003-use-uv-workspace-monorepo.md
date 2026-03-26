# ADR-0003: Use a uv workspace monorepo

Date: 2026-03-26  
Status: Accepted

## Context

VertexLab is organized as multiple related packages with shared tooling, CI policies, and docs. The project needed a single dependency and task workflow across packages without duplicating environment setup.

## Decision

Use a uv workspace monorepo as the repository structure and package-management baseline.

## Consequences

- Shared tooling and dependency workflows are unified at the workspace root.
- Cross-package CI/lint/typecheck behavior is easier to standardize.
- Contributors get a single setup path for local development.
- Changes to workspace-level config can affect all packages and require coordinated review.
