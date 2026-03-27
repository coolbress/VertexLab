# Testing Guide

This guide explains how to run tests, understand test structure, and write new tests for vertex-forager.

> Note
> For the monorepo-wide test layout, markers (`smoke`/`manual`), and CI policy, see the repository guide:
> `packages/vertex-forager/tests/README.md`. This page focuses on vertex-forager-specific workflows.

## Prerequisites

```bash
git clone https://github.com/coolbress/VertexLab.git
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

```text
packages/vertex-forager/tests/
├── conftest.py                 # Shared fixtures (mock clients, sample data)
├── unit/                       # Fast tests with mocked dependencies
├── integration/                # End-to-end with real writers
├── writers/                    # DuckDB-specific tests
├── verification/               # Performance verification scripts
└── examples/                   # Provider matrix smoke test
```

## Test Classification

### Unit tests

Fast, isolated tests with mocked dependencies. These form the bulk of the test suite.

### Integration tests

End-to-end tests exercise real writers, pipelines, or CLI paths and use `@pytest.mark.integration`.

!!! note
  Integration tests are excluded from the default CI run (`-m "not integration"`) to keep the feedback loop fast. Run them locally before submitting PRs that change writers or pipeline logic.

## Property-Based Testing

The project uses [Hypothesis](https://hypothesis.readthedocs.io/en/latest/) for property-based testing to verify invariants across random inputs.

Use Hypothesis when testing functions that should satisfy invariants for any valid input, such as normalization, parsing, and serialization.

## Writing New Tests

- File naming: `test_<module>.py`
- Function naming: `test_<behavior_under_test>`
- Use `_property` suffix for Hypothesis tests
- Use `tmp_path` for filesystem isolation
- Mark slow or real I/O tests with `@pytest.mark.integration`

## Next Steps

- Review contributor workflow: [Contributing](https://coolbress.github.io/VertexLab/contributing/)
- Read the tutorial: [Quickstart](../tutorials/quickstart.md)
- Check architecture details: [Pipeline architecture](../explanation/architecture.md)
