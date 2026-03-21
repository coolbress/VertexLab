# Testing Guide

This guide explains how to run tests, understand test structure, and write new tests for vertex-forager.

## Prerequisites

```bash
git clone https://github.com/coolbress/vertex-lab.git
cd vertex-lab
uv sync --dev
```

## Running Tests

### All tests (excluding integration)

```bash
uv run pytest packages/vertex-forager/tests/ -m "not integration"
```

### Integration tests only

```bash
uv run pytest packages/vertex-forager/tests/ -m integration
```

### With coverage report

```bash
uv run pytest packages/vertex-forager/tests/ \
  --cov=packages/vertex-forager/src/vertex_forager \
  --cov-report=term-missing \
  -m "not integration"
```

### Single test file

```bash
uv run pytest packages/vertex-forager/tests/unit/test_config.py -v
```

## Test Directory Structure

```
packages/vertex-forager/tests/
├── conftest.py                 # Shared fixtures (mock clients, sample data)
├── unit/                       # ~36 files — fast, no external dependencies
├── integration/                # ~8 files — end-to-end with real writers
├── writers/                    # ~2 files — DuckDB-specific tests
├── verification/               # ~3 files — performance verification scripts
└── examples/                   # ~1 file — provider matrix smoke test
```

## Test Classification

### Unit tests (`tests/unit/`)

Fast, isolated tests with mocked dependencies. These form the bulk of the test suite.

```python
def test_schema_mapper_strict_raises_on_missing_columns(pkt_factory) -> None:
  df = pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"]})
  mapper = SchemaMapper(strict_validation=True)
  pkt = pkt_factory("yfinance_info", df)
  with pytest.raises(ValueError, match=r"Schema validation failed"):
    mapper.normalize(pkt)
```

### Integration tests (`tests/integration/`)

End-to-end tests that exercise real writers, pipelines, or CLI. Marked with `@pytest.mark.integration`:

```python
@pytest.mark.asyncio
@pytest.mark.integration
async def test_pipeline_records_writer_validation_errors(
  tmp_path: Path, monkeypatch
) -> None:
  monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))
  # ... full pipeline with real DuckDB writer ...
```

!!! note
  Integration tests are excluded from the default CI run (`-m "not integration"`) to keep the feedback loop fast. Run them locally before submitting PRs that change writers or pipeline logic.

## Configuration

### pytest settings (`pyproject.toml`)

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = [
  "integration: integration tests",
]
```

- `asyncio_mode = "auto"` — async test functions are detected automatically; `@pytest.mark.asyncio` is optional
- `integration` marker — separates slow/external tests from unit tests

### Coverage settings

```toml
[tool.coverage.run]
branch = true
source = [
  "packages/vertex-forager/src/vertex_forager/core",
  "packages/vertex-forager/src/vertex_forager/clients",
  "packages/vertex-forager/src/vertex_forager/writers",
  "packages/vertex-forager/src/vertex_forager/routers",
]

[tool.coverage.report]
fail_under = 80
show_missing = true
skip_covered = true
```

- **Branch coverage** enabled — both branches of `if/else` must be hit
- **Threshold**: 80% minimum (CI fails below this)
- **Scope**: `core`, `clients`, `writers`, `routers` modules

### Hypothesis settings

```toml
[tool.hypothesis]
max_examples = 50
deadline = 200
```

## Shared Fixtures (`conftest.py`)

Key fixtures available to all tests:

| Fixture | Purpose |
|---------|---------|
| `mock_async_client` | Mocked `httpx.AsyncClient` |
| `mock_http_executor` | `HttpExecutor` with mocked fetch |
| `sharadar_client` | Pre-configured Sharadar client (mocked HTTP) |
| `sharadar_router` | Sharadar router instance |
| `yfinance_router` | YFinance router (`allow_pickle_compat=False`) |
| `sample_price_data` | Sample Sharadar price JSON payload |
| `sample_ticker_data` | Sample Sharadar ticker JSON payload |
| `pkt_factory` | Factory for `FramePacket` by table name and DataFrame |

Helper functions: `create_mock_response`, `assert_dataframe_structure`, `create_test_fetch_job`, `create_test_frame_packet`.

## Property-Based Testing (Hypothesis)

The project uses [Hypothesis](https://hypothesis.readthedocs.io/) for property-based testing to verify invariants across random inputs:

```python
from hypothesis import given
from hypothesis import strategies as st

@given(st.lists(st.text(min_size=0, max_size=20), min_size=1, max_size=10))
def test_normalize_columns_property(names: list[str]) -> None:
  df = pl.DataFrame({n or "": [] for n in names})
  out = normalize_columns(df)
  assert len(out.columns) == len(df.columns)
  assert all(re.fullmatch(r"[a-z0-9_]+", c) for c in out.columns)
```

Use Hypothesis when testing functions that should satisfy invariants for **any** valid input — normalization, parsing, serialization.

## Writing New Tests

### Naming conventions

- File: `test_<module>.py` (mirrors source module name)
- Function: `test_<behavior_under_test>`
- Use `_property` suffix for Hypothesis tests: `test_normalize_columns_property`

### Async tests

With `asyncio_mode = "auto"`, simply define an `async def test_*` function:

```python
async def test_http_executor_retries_on_429() -> None:
  executor = HttpExecutor(client=mock_client)
  result = await executor.fetch(spec)
  assert result.status_code == 200
```

### Using `tmp_path` for isolation

Tests that write files (DuckDB, DLQ) should use pytest's `tmp_path` fixture:

```python
async def test_duckdb_writer_creates_table(tmp_path: Path) -> None:
  writer = DuckDBWriter(db_path=tmp_path / "test.duckdb")
  await writer.write(packets)
  assert (tmp_path / "test.duckdb").exists()
```

### Marking integration tests

Any test that requires real I/O (filesystem, network) or takes >1 second should be marked:

```python
@pytest.mark.integration
async def test_full_pipeline_with_duckdb(tmp_path, monkeypatch):
  ...
```
