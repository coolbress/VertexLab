# Installation

Install `vertex-lab` when you want the full VertexLab meta-package and a single entrypoint for the broader toolchain.

> Today, `vertex-forager` is the implemented component behind the practical install and quickstart path. `vertex-qt` and `vertex-workspace` remain planned components, and this page will expand as they ship.
> Current distribution model: VertexLab packages are distributed through GitHub Releases and repository-based workflows. PyPI publishing is not enabled yet, but `pip install` still works when you point pip at a GitHub source URL or a release asset URL.

## Install The Meta-package From GitHub

```bash
git clone https://github.com/coolbress/VertexLab.git vertex-lab
cd vertex-lab
uv sync --group dev
```

Install `vertex-lab` this way if you want the full repository workflow, the root documentation experience, and the current GitHub-based distribution path.

If you want a direct `pip install` path without PyPI, use a GitHub release asset URL for a specific tag:

```bash
pip install https://github.com/coolbress/VertexLab/releases/download/<tag>/vertex_lab-<version>-py3-none-any.whl
pip install https://github.com/coolbress/VertexLab/releases/download/vertex-lab-v0.3.6/vertex_lab-0.3.6-py3-none-any.whl
```

## Install Individual Packages

Use package-specific installs when you only need one part of the stack.

Choose one of the following package install variants depending on the workflow you need.

```bash
# basic
pip install "vertex-forager @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
# with yfinance
pip install "vertex-forager[yfinance] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
# with notebook
pip install "vertex-forager[notebook] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

Prefer a release tag such as `vertex-forager-v0.11.4` or a commit SHA for reproducible installs instead of using a moving branch name.

Use package-specific docs when you want a direct workflow for an individual component:

- `vertex-forager`: [installation docs](https://coolbress.github.io/VertexLab/vertex-forager/get-started/installation/)
- `vertex-qt`: docs will be added when the package implementation is ready

- `vertex-forager` handles data collection, schema-aware normalization, and DuckDB persistence
- `vertex-qt` is planned for quantitative analysis and backtesting workflows; install guidance will be added once the package is ready
- `vertex-workspace` is planned for the local dashboard and workflow orchestration UI; install guidance will be added once the package is ready

## Choose The Right Install Path

| Use case | Recommended install |
| --- | --- |
| You want the top-level VertexLab package and the root project docs experience | Clone the repo and run `uv sync --group dev` |
| You want the current production-ready data ingestion workflow right now | Install `vertex-forager` from GitHub and follow the [vertex-forager installation docs](https://coolbress.github.io/VertexLab/vertex-forager/get-started/installation/) |
| You need the yfinance-backed provider path | Install `vertex-forager[yfinance]` from GitHub |
| You want notebook execution support for examples and experiments | Install `vertex-forager[notebook]` from GitHub |
| You are waiting for future quant-analysis package guidance | `vertex-qt` docs will be added when the package is ready |
| You are contributing code, docs, or tests in this repository | `uv sync --group dev` |

## Local Development Install

```bash
git clone https://github.com/coolbress/VertexLab.git vertex-lab
cd vertex-lab
uv sync --group dev
```

## Environment Variables

| Variable | Purpose | Applies to |
| --- | --- | --- |
| `SHARADAR_API_KEY` | Enables Sharadar-backed data collection | `vertex-forager` |
| `VF_PROVIDER` | Selects the provider for quickstart examples | `vertex-forager` |
| `VF_TICKERS` | Supplies tickers for quickstart examples | `vertex-forager` |
| `VF_DUCKDB_PATH` | Chooses the local DuckDB output path | `vertex-forager` |

## Support And Release Notes

- Python support: VertexLab targets Python 3.10 and newer.
- Release expectations: release automation attaches package artifacts to GitHub Releases and updates the root `CHANGELOG.md`.
- Stability note: `vertex-forager` is the production-ready package today; `vertex-qt` and `vertex-workspace` will gain dedicated install and support guidance as they mature.
- Packaging note: package-name installs such as `pip install vertex-lab` depend on PyPI publishing and are not available yet; GitHub-source installs and release-asset installs are supported today.

## Next Steps

- Run the current data workflow: [Quickstart](quickstart.md)
- Explore package docs: [vertex-forager](https://coolbress.github.io/VertexLab/vertex-forager/)
- Contribute changes: [Contributing](contributing.md)
