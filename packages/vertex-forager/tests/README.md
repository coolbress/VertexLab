# Tests Guide

## Structure
- unit: fast, deterministic tests (default in CI)
  - http, writers, cli, pipeline, utils, etc.
- integration: multi-component interactions (still offline by default)
- property: Hypothesis-based invariants and boundary checks
- verification: manual/opt-in validations (performance/resource-heavy)

## Markers
- smoke: ultra-fast guards (imports/entrypoints)
- manual: opt-in only (excluded from default CI)

## Local Commands
- Fast suite (default): `pytest packages/ -m "not manual"`
- Manual only: `pytest packages/ -m manual`
- Coverage: `pytest packages/ --cov --cov-fail-under=80`
- Lint & typecheck: `ruff check packages/ --fix && mypy packages/vertex-forager/src --strict`

## CI Policy
- Default run excludes manual tests for speed and determinism.
- Use workflow_dispatch input `include_manual=true` to run manual suite.
