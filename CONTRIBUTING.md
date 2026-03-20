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
  - Note (tests): keep packages/vertex-forager/tests/** exceptions S101 and TC002/TC003/TC006 for pytest assert ergonomics and type-checking imports.

## Lint rules & quick‑fix cheatsheet

- Imports (I001)
  - Keep imports sorted and grouped (stdlib, third‑party, first‑party). Use: `uv run ruff check --select I --fix`.
  - Respect `known-first-party = ["vertex_forager"]` in pyproject.
- Long lines (E501)
  - Max line length is 120. Wrap with parentheses, implicit string concatenation, or helper variables.
  - For parametrized tests, split argument lists/tuples across lines.
- Pyupgrade (UP006/UP035/UP037)
  - Prefer PEP 585 generics: `list[int]` over `typing.List[int]`, `dict[str, Any]` over `typing.Dict[...]`.
  - Use `X | Y` instead of `typing.Union[X, Y]` on Python 3.10+.
  - Remove deprecated typing aliases/imports where safe.
- Pytest style (PT011/PT012/PT015/PT018)
  - Normalize `@pytest.mark.parametrize`:
    - Names as a single comma‑separated string: `"x, y"`.
    - Values as a list of tuples: `[(1, 2), (3, 4)]`.
  - Prefer `with pytest.raises(ExpectedError):` context manager over try/except.
  - Avoid redundant marks/fixtures usage; keep ids stable and concise.
- Type‑checking gates (TC00x)
  - Import heavy/type‑only symbols behind `if TYPE_CHECKING:`; keep runtime imports only for code paths that execute.
  - Example pattern:
    
    ```python
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from collections.abc import Sequence
    ```
    

Quick references:
- pyproject: `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.ruff.lint.per-file-ignores]`
- Tests keep only: S101, TC002, TC003, TC006 under `packages/vertex-forager/tests/**`.

## Temporary per‑file‑ignores policy

- When to add
  - Only as a short‑term unblocker when a file cannot be reasonably fixed within the current PR.
  - Scope to the narrowest path (specific file or subfolder), never repo‑wide.
- How to add (pyproject.toml)
  - Edit `[tool.ruff.lint.per-file-ignores]` and add the minimal rule list for the specific path.
  - Example:
    
    ```toml
    [tool.ruff.lint.per-file-ignores]
    "packages/vertex-forager/src/vertex_forager/some_legacy.py" = ["E501", "PT011"]
    ```
    
- Removal in follow‑up PRs
  - Track ignores in the PR description and open a follow‑up to remove them once code is compliant.
  - Replace ignores with targeted refactors/tests; keep the list shrinking over time.

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

### Labels policy (overview)
- See `docs/explanation/labels-policy.md` for the label taxonomy (type, area, priority) and how to apply labels on PRs/issues.
- The PR template includes required sections to help reviewers (Summary, Risk & Rollback, etc.). Link your lint exceptions and follow‑up cleanups there.

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
