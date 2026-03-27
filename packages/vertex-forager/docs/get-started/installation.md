# Installation

Use `vertex-forager` when you want a packaged entrypoint for data collection, normalization, and persistence workflows.

## Install From PyPI

```bash
pip install vertex-forager
```

## Optional Extras

```bash
pip install "vertex-forager[yfinance]"
pip install "vertex-forager[notebook]"
```

- `yfinance` enables the library-backed provider and notebook-friendly examples
- `notebook` installs the dependencies used for notebook execution

## Install With uv

```bash
uv pip install vertex-forager
```

## Install From The Repository

```bash
git clone https://github.com/coolbress/VertexLab.git
cd VertexLab
uv sync
```

Use the repository workflow when you are developing the package, running the examples, or contributing documentation and tests.

## Next Steps

- Run the tutorial: [Quickstart](../tutorials/quickstart.md)
- Learn the test workflow: [Testing Guide](testing.md)
- Review configuration details: [EngineConfig](../reference/config.md)
