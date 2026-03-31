# VertexLab

[![Docs](https://img.shields.io/badge/docs-online-blue)](https://coolbress.github.io/VertexLab/)
[![Coverage](https://raw.githubusercontent.com/coolbress/VertexLab/python-coverage-comment-action-data/badge.svg)](https://github.com/coolbress/VertexLab/tree/python-coverage-comment-action-data)

Modern Python monorepo for financial data collection, orchestration, analysis, and visualization.

## Repository layout

```text
vertex-lab/
├── .github/                  # Workflows and automation
├── CONTRIBUTING.md           # Contribution guide
├── packages/
│   ├── vertex-lab/           # Thin orchestration package and CLI shell
│   ├── vertex-forager/       # Data collection engine
│   ├── vertex-qt/            # Quant analysis package
│   └── vertex-workspace/     # Workspace/dashboard package
├── scripts/                  # Repository helpers
├── mkdocs.yml                # Shared docs site navigation
├── pyproject.toml            # Workspace configuration and dev tooling
└── uv.lock                   # Shared workspace lockfile
```

## Development setup

```bash
git clone https://github.com/coolbress/VertexLab.git vertex-lab
cd vertex-lab
uv sync --group dev
```

## Packages

- `vertex-lab`: thin user-facing orchestration package and future app entrypoint
- `vertex-forager`: production-ready data collection package
- `vertex-qt`: quantitative analysis package
- `vertex-workspace`: workspace/dashboard package

## Documentation

- Shared docs portal: <https://coolbress.github.io/VertexLab/>
- `vertex-lab` package docs: <https://coolbress.github.io/VertexLab/>
- `vertex-forager` package docs: <https://coolbress.github.io/VertexLab/vertex-forager/>

## License

Apache-2.0 License - see LICENSE for details.
