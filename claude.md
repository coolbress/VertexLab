# Vertex Lab · Claude Code CLI Guardrails

This document defines repository-level guardrails for using Claude Code CLI. It emphasizes token efficiency, high‑level analysis, and safe, minimal changes aligned with industry standards.

## Purpose & Scope

- Purpose: Read only what’s necessary, reason at the right abstraction level, and make safe, minimal edits.
- Scope: Python 3.10+ monorepo using uv workspace under packages/*.

## Output Contract

- Language: General chat in Korean; code comments (if needed), Issues/PRs, and commit messages in English.
- Format for all assistant outputs:
  1) Conclusion (summary of changes or decision)
  2) Related file links in file:///ABS/PATH#Lx-Ly form
  3) Next actions (commands to run)
- Example link (placeholder): [api.py](file:///$REPO_ROOT/packages/vertex-forager/src/vertex_forager/api.py#L10-L35)
- Links must use absolute file:/// URLs.
- Commands are written to be executed from the repository root; use repo‑root‑relative paths or explicit cd instructions.

## Exploration Strategy

- No recursive listing (e.g., ls -R). Identify target files first, then read only required line ranges.
- Never load entire long files unless unavoidable; prefer precise ranges.
- Start with high-level questions (e.g., “auth flow”, “error handling policy”), then split into focused sub-queries.

## Change Rules

- Prefer existing style and dependencies; introduce or replace libraries only when they demonstrably improve performance, reliability, security, or maintainability and the dependency checklist is satisfied.
- Edit only code directly related to the requested feature/bug; avoid opportunistic refactors/renames.
- PEP 8, Google-style docstrings, mypy for strict static type checking and Pydantic for runtime validation, and pathlib.Path only.
- Do not add comments in code unless explicitly requested.

### Dependency Change Checklist (Required)

- Issue opened and linked with clear rationale and scope.
- Benchmarks/profiling show material improvement on representative workloads.
- Security and license review completed; supply‑chain risks considered.
- API stability and ecosystem maturity evaluated.
- Minimal surface-area change with migration and rollback plan.
- Manage with uv from the package directory; pin versions with appropriate bounds:
  - Core/runtime dependencies: apply upper bounds per repo policy.
  - Dev tools (lint/test/etc.): lower-bound pins are acceptable.
- All quality gates pass (ruff, mypy, pytest, cycle checks).

## Industry-Grade Workflow

- Issue (English): Create an issue with gh issue create (Bug/Feature/Docs/Tech template) and capture the issue number.
- Branching: Work on feat/, fix/, or refactor/ with the issue number; never push to main directly.
- PR (English): Use gh pr create and include the eight mandatory sections:
  - Summary, Linked Issue, Type of Change, Changes, Verification, Security Considerations, Risk & Rollback, Checklist
- Commits (English): Follow Conventional Commits `<type>(<scope>)?: <desc> (#issue)`; merge via Squash.
  - Type is required; Scope is optional.
  - PR titles must end with an issue number in the form `(#123)`.
  - Allowed types: build, chore, ci, docs, feat, fix, perf, refactor, revert, style, test.
  - Squash Merge uses the PR title as the final commit message.
- uv.lock conflicts (monorepo): Prefer deterministic re-lock from repo root — remove the conflicted lock and regenerate the lock file:
  - From repository root: rm uv.lock && uv lock
  - Note: uv sync is not required for resolving lock-file merge conflicts
- Environment sync (post-merge): uv sync [--group dev]

## Pre-change Safety Checks

- Always run:
  - git fetch origin
  - git diff origin/main -- <file> (check hotspots before editing)
- Directory deletion safeguards:
  - Resolve paths; ensure target is under repo root
  - Protect root/home; verify existence and directory type

## Quality Gates (Must Pass Locally)

- All checks below are required (mandatory).
- Run location: repository root
- Command paths: use repo‑root‑relative paths or explicit cd
- Lint: uv run ruff check packages/ --fix
- Security lint (mandatory): uv run ruff check packages/ --select S
- Types: uv run mypy packages/vertex-forager/src --strict
- Tests: uv run pytest packages/ --cov-fail-under=80
- Cycles: uv run python scripts/check_cycles.py
- Secrets scan (mandatory): uv run detect-secrets scan --all-files

## Security Principles

- Never include secrets (API keys, tokens, credentials) in CLAUDE.md, code, or logs.
- Manage sensitive configuration via environment variables; do not commit .env files.
- Avoid printing or storing secrets; scrub logs if accidental exposure occurs.
- Document only the principle (“manage via environment variables”) rather than concrete secret values.

## Analysis Ignore Policy

- .claudeignore complements .gitignore to exclude large, non-code artifacts (datasets, logs, reports, media) from AI analysis.
- Avoid excluding source code needed for understanding or refactoring.
- Keep rules minimal and project-wide; refine patterns as team policy evolves.

## Code Evaluation & Optimization

- Evaluate across six areas: correctness, data integrity, security, performance, reliability, and code quality.
- Architecture: Follow SOLID and protocol-based abstractions for replaceable implementations.
- Resource guidelines:
  - Network: Use an async client, manage connection pools, implement exponential backoff retries.
  - Data: Prefer vectorized operations and memory-efficient engines based on dataset scale.
  - Concurrency: Respect rate limits and system constraints; avoid event-loop blocking.

## Benchmarking & Profiling

- Compare against context-appropriate industry references automatically selected by the assistant (e.g., time‑series data sources, dataframe engines, backtesting frameworks), and document the rationale for chosen baselines.
- Metrics: Latency, Memory Footprint, Throughput. Demonstrate improvements via benchmarks or profiles.
- Internal profiling example:
  - uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf.py
  - Output: tests/verification/profile_metrics.json

## Prompt Snippets (Copy-Paste)

Architecture Review (≤5 top risks)
PlainText
Goal:
- Identify top risks across correctness, integrity, security, performance, reliability, and quality.
Output:
- Risk summary with proposals
- Minimal file links
- Next actions (commands/edits)
---

Bug Fix (Minimal Change)
PlainText
Goal:
- Repro → suspect cause (1–2) → minimal fix
Verify:
- ruff/mypy/pytest/cycle checks pass
Output:
- Change summary → file links → commands
---

Performance Tuning (Top-2 Bottlenecks)
PlainText
Goal:
- Pinpoint bottlenecks → safe tuning plan (concurrency/Polars/httpx)
Verify:
- Compare profiling metrics (p95/rows/memory)
Output:
- Summary → links → commands
---
