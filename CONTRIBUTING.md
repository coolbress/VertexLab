# Contributing

Thank you for contributing to Vertex Forager!

## Development quickstart
- Install uv and set up the workspace:
  - `uv sync -g dev`
- Install and enable Git hooks:
  - `pre-commit install`
- Run quality gates locally:
  - `uv run ruff check packages/`
  - `uv run mypy packages/vertex-forager/src --strict`
  - `uv run pytest packages/ -q`

## Documentation
- Docs live under `packages/vertex-forager/docs` (MkDocs + Material, Diátaxis).
- Build locally:
  - `uv pip install "mkdocs>=1.6.0" "mkdocs-material>=9.5.0" "mkdocstrings[python]>=0.25.0" "pymdown-extensions>=10.8"`
  - `uv run mkdocs serve`
- API reference uses mkdocstrings. Please add minimal Google-style docstrings for public APIs.

## Commit/PR guidelines
- Use conventional prefixes (guard‑approved): `feat:`, `fix:`, `docs:`, `refactor:`, `perf:`, `test:`, `chore:`.
- Add tests for user-visible changes when applicable.
- Keep provider-specific logic in provider modules; avoid bleeding into core.
- Follow the PR template: include Summary, Linked Issue, Type of Change, Changes, Verification, Security Considerations, Risk & Rollback, Breaking Change?, Screenshots/CLI Output, and Checklist.
- Ensure public API changes include docstrings; place tests under `packages/vertex-forager/tests/`.

## Security
- Never commit secrets. Use environment variables or secret managers.
- Report vulnerabilities via `SECURITY.md`.
