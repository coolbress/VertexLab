---
title: Labels Policy — scope:* Unification
---

Goal
- Unify path/function taxonomy on a single axis using `scope:*` labels.

Policy
- Use only `scope:*` for path/function classification:
  - scope:docs, scope:ci, scope:pipeline, scope:writer,
  - scope:provider:yfinance, scope:provider:sharadar
- Do not add `area:*` labels in new PRs.
- Keep `type:*`, `severity/*`, `priority/*`, `impact:*` and no-changelog as-is.

Migration (legacy → scope)
- area:docs → scope:docs
- area:ci → scope:ci
- area:core, area:routers → scope:pipeline
- area:writers → scope:writer
- area:clients / area:providers → `scope:provider:*` (or generic `scope:provider` if adopted)

Rollout
1) Stop adding `area:*` in auto labelers (labeler.yml + workflow fallback).
2) Backfill historical PRs (gh CLI) to replace `area:*` with `scope:*`.
3) Verify “0 usage” of `area:*` labels, then remove them from the repository.
4) Keep this document referenced from contributor guidelines.

Notes
- Title → `type:*` remains inferred from Conventional Commits (feat/fix/docs/etc.).
- If mixed provider changes occur, allow multiple `scope:provider:*` labels.
