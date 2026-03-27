---
title: Labels Policy — type:* and package:* Automation
---

Goal

- Keep labels consistent, low-maintenance, and fully automated.
- Use only two managed axes for PR labeling: `type:*` and `package:*`.

Managed label taxonomy

- `type:*`
  - `type:feature`
  - `type:fix`
  - `type:docs`
  - `type:perf`
  - `type:refactor`
  - `type:chore`
  - `type:breaking`
- `package:*`
  - `package:vertex-forager`
  - `package:vertex-qt`
  - `package:vertex-workspace`
  - `package:vertex-lab`
- Optional release-control label
  - `no-changelog`

Automation behavior

- PR title validation uses Conventional Commit style:
  - `type(scope)?: summary`
  - Allowed types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`
- `type:*` labels are inferred from title:
  - `feat` → `type:feature`
  - `fix` → `type:fix`
  - `docs` → `type:docs`
  - `perf` → `type:perf`
  - `refactor` → `type:refactor`
  - `chore` / `ci` / `build` / `test` / `style` / `revert` → `type:chore`
  - `feat!` or `BREAKING CHANGE` in body → `type:breaking`
- `package:*` labels are inferred from changed paths:
  - `packages/vertex-forager/**` → `package:vertex-forager`
  - `packages/vertex-qt/**` → `package:vertex-qt`
  - `packages/vertex-workspace/**` → `package:vertex-workspace`
  - `pyproject.toml`, `.github/**`, `src/vertex_lab/**`, `scripts/**` → `package:vertex-lab`

Contributor expectations

- Do not use or introduce `scope:*`, `area:*`, `severity/*`, or `priority/*` labels.
- Do not manually set managed `type:*` / `package:*` labels unless automation is unavailable.
- Keep PR titles aligned with the validation rule so automation can label correctly.
