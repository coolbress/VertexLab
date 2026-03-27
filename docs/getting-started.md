# Getting Started

This page helps you choose the right entrypoint for VertexLab.

## Choose Your Path

- Evaluating the repository structure and policies: start with the [Overview](index.md)
- Working on repository changes: read [Contributing](contributing.md)
- Using the main package today: go to [vertex-forager](https://coolbress.github.io/VertexLab/vertex-forager/)
- Looking for release history: open [CHANGELOG.md](https://github.com/coolbress/VertexLab/blob/main/CHANGELOG.md)

## Local Setup

1. Sync the workspace:

   ```bash
   uv sync
   ```

2. Run the core repository checks:

   ```bash
   uv run ruff check packages/ --fix
   uv run mypy packages/vertex-forager/src --strict
   uv run pytest packages/ -m smoke
   ```

3. Build the docs site when changing documentation:

   ```bash
   NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict
   ```

   If your environment does not already have the MkDocs tooling installed through the project dependencies, run the explicit form instead:

   ```bash
   NO_MKDOCS_2_WARNING=1 uv run --with mkdocs --with mkdocs-material --with mkdocs-monorepo-plugin --with 'mkdocstrings[python]' --with pymdown-extensions mkdocs build --strict
   ```

## Main Component

### vertex-forager

`vertex-forager` is the current user-facing package in this monorepo.

- Docs home: [vertex-forager](https://coolbress.github.io/VertexLab/vertex-forager/)
- Quickstart: [tutorials/quickstart.md](https://coolbress.github.io/VertexLab/vertex-forager/tutorials/quickstart/)
- API reference: [reference/api.md](https://coolbress.github.io/VertexLab/vertex-forager/reference/api/)

## Repository Navigation

- Overview: repository portal and component map
- Contributing: contribution workflow, PR rules, and release notes
- Policies: CI, labels, and workflow governance
- ADRs: architecture decision records for major technical choices
