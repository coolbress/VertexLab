# Contributing

Thank you for contributing to Vertex Forager!

## Development quickstart
- Install uv and set up the workspace:
  - `uv sync`
- Install and enable Git hooks:
  - `pre-commit install`
- Keep dev tools fresh:
  - Run `pre-commit autoupdate` monthly (or open a “dev‑tools refresh” PR) to bump hook versions like ruff, yamllint, lychee. Review diffs and pin to stable tags.
- Run quality gates locally:
  - `uv run ruff check packages/`
  - `uv run mypy packages/vertex-forager/src --strict`
  - `uv run pytest packages/ -q`
  - Note: pre-commit runs mypy against the same target (`packages/vertex-forager/src`) with `--strict` to ensure local and CI type errors match. Install hooks via `pre-commit install`.

## Documentation
- Docs live under `packages/vertex-forager/docs` (MkDocs + Material, Diátaxis).
- Build locally:
  - `uv pip install "mkdocs>=1.6.0" "mkdocs-material>=9.5.0" "mkdocstrings[python]>=0.25.0" "pymdown-extensions>=10.8"`
  - `uv run mkdocs serve`
- API reference uses mkdocstrings. Please add minimal Google-style docstrings for public APIs.
- Link checking locally (optional):
  - macOS: `brew install lychee`
  - Rust: `cargo install lychee --locked`
  - Run: `pre-commit run lychee -a`

## Commit/PR guidelines
- Use conventional prefixes (guard‑approved): `feat:`, `fix:`, `docs:`, `refactor:`, `perf:`, `test:`, `chore:`.
- Add tests for user-visible changes when applicable.
- Keep provider-specific logic in provider modules; avoid bleeding into core.
- Follow the PR template: include Summary, Linked Issue, Type of Change, Changes, Verification, Security Considerations, Risk & Rollback, Breaking Change?, Screenshots/CLI Output, and Checklist.
- Ensure public API changes include docstrings; place tests under `packages/vertex-forager/tests/`.

## Security
- Never commit secrets. Use environment variables or secret managers.
- Report vulnerabilities via `SECURITY.md`.

## CI Policy (Actions Pinning)

- Pin GitHub Actions by SHA; include a comment noting the upstream major (e.g., checkout v5 SHA).
- Use Node 24‑compatible action versions; avoid floating tags for reproducibility and supply‑chain safety.

## Lockfile Policy (uv.lock)

- Do not include uv.lock changes in non‑dependency PRs; keep lockfile updates to deps/ci maintenance.
- When updating dependencies, include uv.lock changes produced by `uv sync` to maintain reproducibility.
- Prefer small, focused lockfile diffs; avoid mixing code changes with large dependency churn.
