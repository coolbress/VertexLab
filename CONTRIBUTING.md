# Contributing

Thank you for contributing to Vertex Forager!

## Development quickstart
- Install uv and set up the workspace:
  - `uv sync -g dev`
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
- Use conventional prefixes: `[docs]`, `[fix]`, `[feat]`, `[refactor]`, `[ci]`, `[chore]`.
- Add tests for user-visible changes when applicable.
- Keep provider-specific logic in provider modules; avoid bleeding into core.

## Security
- Never commit secrets. Use environment variables or secret managers.
- Report vulnerabilities via `SECURITY.md`.
