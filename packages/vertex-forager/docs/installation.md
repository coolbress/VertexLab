# Installation

Use `vertex-forager` when you want a packaged entrypoint for data collection, normalization, and persistence workflows.

> vertex-forager is currently distributed from GitHub. Prefer a release tag or commit SHA for reproducible installs.

## Install From GitHub

```bash
pip install "vertex-forager @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

## Optional Extras

```bash
pip install "vertex-forager[yfinance] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
pip install "vertex-forager[notebook] @ git+https://github.com/coolbress/VertexLab.git@vertex-forager-v0.11.4#subdirectory=packages/vertex-forager"
```

- `yfinance` installs the library-backed provider dependencies: `pandas` and `yfinance`
- `notebook` installs the notebook runtime dependencies: `ipywidgets`, `ipython`, and `nest-asyncio`

You can also install from a wheel or sdist attached to [GitHub Releases](https://github.com/coolbress/VertexLab/releases).

## Install From The Repository

```bash
git clone https://github.com/coolbress/VertexLab.git
cd VertexLab
uv sync --group dev
```

Use the repository workflow when you are developing the package, running the examples, or contributing documentation and tests.

## Next Steps

- Run the tutorial: [Quickstart](tutorials/quickstart.md)
- Learn the test workflow: [Testing Guide](how-to/testing.md)
- Review configuration details: [EngineConfig](reference/config.md)
