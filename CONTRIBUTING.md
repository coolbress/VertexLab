# Contributing

Thank you for contributing to VertexLab.

## Before You Start

- Start with an issue before opening a PR for any non-trivial change.
- Use one of the four issue forms:
  - Bug report
  - Feature request
  - Docs improvement
  - Maintenance / CI / Chore
- Use a plain issue title. Do not add Conventional Commit prefixes to issue titles.
- Link the issue in your PR body with `Closes #<id>`.

## Development Quickstart

1. Sync the workspace:

   ```bash
   uv sync
   ```

2. Install pre-commit hooks:

   ```bash
   pre-commit install
   ```

3. Run the required local quality gates from the repository root:

   ```bash
   uv run ruff check packages/ --fix
   uv run mypy packages/vertex-forager/src --strict
   uv run pytest packages/ --cov-fail-under=80
   ```

4. Use the smoke marker for quick import and entrypoint checks:

   ```bash
   uv run pytest packages/ -m smoke
   ```

## Issue Guidelines

- Bug report: use for defects, regressions, and broken workflows.
- Feature request: use for new functionality or product changes.
- Docs improvement: use for documentation corrections, additions, or restructures.
- Maintenance / CI / Chore: use for refactors, CI, release, tooling, and non-feature cleanup.
- Keep scope narrow. If work splits naturally, open follow-up issues.

## Branch Naming

- Create branches from the latest `main`.
- Use one of these prefixes:
  - `feat/issue-<id>-short-slug`
  - `fix/issue-<id>-short-slug`
  - `docs/issue-<id>-short-slug`
  - `chore/issue-<id>-short-slug`
- Use `refactor/` only when the work is clearly structural and not feature or fix oriented.
- Do not commit directly to `main`.

## Commit And PR Guidelines

- Use Conventional Commits for commits and PR titles.
- Allowed types:
  - `build`
  - `chore`
  - `ci`
  - `docs`
  - `feat`
  - `fix`
  - `perf`
  - `refactor`
  - `revert`
  - `style`
  - `test`
- PR title format:

  ```text
  type(scope)?: summary
  ```

- A breaking change uses `!` after the type or scope:

  ```text
  feat(vertex-lab)!: change CLI startup behavior
  ```

- You can also declare breaking behavior in the PR body with a `BREAKING CHANGE:` footer line.
- PR bodies must follow the 5-section template:
  - Summary
  - Linked Issue
  - Changes
  - Verification
  - Checklist
- Use squash merge. The PR title becomes the final merge commit title.
- Do not append issue numbers to the PR title. Link issues in the PR body instead.

## Labels

VertexLab keeps pull-request labels low-maintenance and automation-friendly by managing only `type:*` and `package:*` labels in normal workflow.

| Label Group | Labels |
| --- | --- |
| `type:*` | `type:feature`, `type:fix`, `type:docs`, `type:perf`, `type:refactor`, `type:chore`, `type:breaking` |
| `package:*` | `package:vertex-forager`, `package:vertex-qt`, `package:vertex-workspace`, `package:vertex-lab` |
| Release control | `no-changelog` |

- `pr-automation.yml` maps PR titles to `type:*` labels and changed paths to `package:*` labels.
- Keep PR titles aligned with the Conventional Commit rule so labels can be applied automatically.
- Do not manually introduce extra label systems such as `scope:*`, `area:*`, `severity/*`, or `priority/*` unless repository policy changes.
- `package:vertex-lab` is now tied to `packages/vertex-lab/**`.

## Pre-commit Hooks

| Hook | Purpose | Notes |
| --- | --- | --- |
| `ruff-format` | Formats Python code | Runs on Python files |
| `ruff-lint` | Applies Ruff lint fixes | Runs on Python files |
| `mypy` | Enforces strict type checking for `vertex-forager` | Runs without filenames |
| `yamllint` | Validates workflow and package YAML files | Manual stage |
| `detect-secrets` | Prevents secret leaks | Uses `.secrets.baseline` |
| `hotspot-diff-check` | Checks high-risk files before commit | Always runs |
| `lychee` | Checks markdown links | Manual stage |
| `codespell` | Fixes spelling issues | Skips lockfile and binary-like files |
| `validate-pyproject` | Validates `pyproject.toml` | Standard config validation |
| `markdownlint-cli2` | Lints markdown files | Includes docs and PR template |
| `end-of-file-fixer` | Normalizes trailing EOF newlines | Standard hygiene |
| `trailing-whitespace` | Removes trailing whitespace | Standard hygiene |
| `mixed-line-ending` | Normalizes line endings to LF | Standard hygiene |
| `mdformat` | Formats markdown | Manual stage |
| `pretty-format-yaml` | Formats YAML files | Manual stage |

## AI Contribution Policy

- AI-generated changes are allowed, but the contributor remains fully responsible for correctness and security.
- Review generated code before committing it.
- Keep prompts, outputs, and edits aligned with repository policies in `claude.md`.
- Never paste secrets, tokens, credentials, or private data into prompts or generated files.
- Prefer small, reviewable PRs over large generated rewrites.
- Document assumptions in the PR body when AI assistance made implementation choices.

## Docs And CI Notes

- Build docs locally when changing documentation or navigation:

  ```bash
  NO_MKDOCS_2_WARNING=1 uv run --with 'packages/vertex-forager[docs]' --with mkdocs-monorepo-plugin mkdocs build --strict
  ```

- CI-required checks live in the main quality-gate path. A passing standalone workflow is not a substitute for the protected gate.
- `pr-automation.yml` validates PR titles and applies `type:*` and `package:*` labels automatically.

## uv.lock Conflicts

- Do not commit `uv.lock` changes in non-dependency PRs.
- If a merge conflict happens in `uv.lock`, regenerate it instead of hand-editing it:

  ```bash
  rm uv.lock
  uv lock
  ```

- Re-run the relevant verification commands after regenerating the lockfile.

## Release Process Overview

- Release automation is handled by release-please.
- Component versions are tracked in `.release-please-manifest.json`.
- For `vertex-lab`, version metadata must stay aligned across:
  - `packages/vertex-lab/pyproject.toml`
  - `packages/vertex-lab/src/vertex_lab/__init__.py`
  - `.release-please-manifest.json`
- Changelog updates for releases are generated by release-please on merge.
- `vertex-lab` release history now lives in `packages/vertex-lab/CHANGELOG.md`.

## Security

- Never commit secrets or tokens.
- Use environment variables or GitHub secrets for sensitive configuration.
- Follow `SECURITY.md` for vulnerability reporting.
