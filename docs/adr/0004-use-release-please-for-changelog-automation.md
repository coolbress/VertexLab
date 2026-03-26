# ADR-0004: Use release-please for changelog automation

Date: 2026-03-26  
Status: Accepted

## Context

Manual changelog updates are error-prone and can drift from release commits. The repository needed a reproducible release note process tied to merge history and versioning flow.

## Decision

Use release-please as the changelog and release-note automation mechanism.

## Consequences

- Changelog generation is consistent and linked to merged commit history.
- Manual changelog maintenance burden is reduced.
- Release process depends on release-please workflow health and configuration.
- Commit and PR metadata quality becomes more important for readable release notes.
