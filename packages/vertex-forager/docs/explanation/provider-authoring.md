# Provider authoring

This page documents the internal steps required to add a new built-in provider to `vertex-forager`.

It is written for maintainers of the package, not for end users. Users are expected to consume the built-in providers shipped by the project rather than develop third-party providers as part of the normal workflow.

The current provider shape is intentionally direct:

- a provider package under `src/vertex_forager/providers/<name>/`
- a `BaseClient` subclass
- a `BaseRouter` subclass
- schema definitions registered through `schema/registry.py`
- client and router wiring in the factory modules

This means a new provider is mostly a matter of creating a predictable set of files and then wiring them into the existing dispatch points.

## 1. Create the provider package

Create a new directory:

```text
src/vertex_forager/providers/<name>/
├── __init__.py
├── client.py
├── router.py
├── schema.py
└── constants.py
```

Recommended responsibilities:

- `__init__.py`
  - export the public provider client and router types
  - keep public package imports stable
- `client.py`
  - define the `BaseClient` subclass
  - validate provider-specific configuration
  - expose supported datasets and dataset specs
- `router.py`
  - define the `BaseRouter` subclass
  - translate provider-local dataset requests into `RequestSpec`
  - normalize provider responses into `FramePacket` values
- `schema.py`
  - define `TableSchema` and `DatasetSpec` objects
  - keep provider-facing dataset names mapped to stable internal tables
- `constants.py`
  - keep provider-specific defaults, limits, and literal constants out of the client and router bodies

If the provider uses a local library transport instead of plain HTTP, add any extra fetcher or transport adapter file alongside these modules.

## 2. Implement the router contract

Subclass `BaseRouter` and follow the existing built-in examples:

- `src/vertex_forager/providers/yfinance/router.py`
- `src/vertex_forager/providers/sharadar/router.py`

The router is responsible for:

- declaring the provider name
- validating dataset-specific arguments
- building `RequestSpec` instances
- parsing responses into normalized `FramePacket` values
- using the provider schema registry entries to resolve canonical table names

Router tests should focus on parse and request-building behavior:

- request parameter construction
- paging/batching behavior
- parse success and parse failure paths
- unsupported dataset and invalid argument handling

## 3. Implement the client contract

Subclass `BaseClient` in `client.py`.

Every provider client must now implement:

- `get_supported_datasets()`
- `get_dataset_spec(dataset)`

Those methods are part of the `BaseClient` abstract contract and should reflect the entries registered in `schema/registry.py`.

Use the built-in clients as references:

- `src/vertex_forager/providers/yfinance/client.py`
- `src/vertex_forager/providers/sharadar/client.py`

The client should own:

- provider credentials and configuration validation
- provider-specific defaults
- dataset-to-table resolution helpers
- orchestration helpers that call `run_pipeline(...)`

## 4. Define schemas and datasets

In `schema.py`, define:

- provider `TABLES`
- provider `DATASETS`

Use:

- `TableSchema` for the internal normalized table contract
- `DatasetSpec` for the provider-local dataset contract

Pattern examples:

- `src/vertex_forager/providers/yfinance/schema.py`
- `src/vertex_forager/providers/sharadar/schema.py`

Keep these rules in mind:

- `DatasetSpec.schema` must point at a valid `TableSchema`
- if `date_filter_col` is omitted, it now defaults from `TableSchema.analysis_date_col`
- unique keys and date columns should match the normalized sink contract, not the provider payload shape

## 5. Register the provider schema

Add the new provider to `src/vertex_forager/schema/registry.py`.

You need to:

- import the provider `TABLES` and `DATASETS`
- merge them into the provider registry maps
- make sure `get_dataset_spec(...)` and `get_provider_dataset_names(...)` can resolve the new provider

This is the canonical source for provider dataset lookup.

## 6. Wire direct dispatch

### Client factory

Update `src/vertex_forager/clients/__init__.py`:

- add the provider to the direct dispatch branch in `create_client(...)`
- extend any overloads or literal-based return typing as needed

### Router factory

Update `src/vertex_forager/routers/__init__.py`:

- add a provider-specific router factory
- add the provider to the direct dispatch branch in `create_router(...)`

If the provider uses a local library transport path, also wire the provider-specific fetch helper into the internal scheme handling path used by `HttpExecutor`.

## 7. Update public typing and exports

Review whether the provider needs updates in:

- `src/vertex_forager/core/types.py`
- `src/vertex_forager/api.py`
- `src/vertex_forager/__init__.py`
- `providers/<name>/__init__.py`

If the provider introduces a new dataset literal or public client type, keep those exports aligned.

## 8. Update docs and CLI surfaces

Review whether the provider must also be added to:

- `docs/reference/providers.md`
- `docs/explanation/how-built-in-providers-work.md`
- `mkdocs.yml`
- `src/vertex_forager/cli.py` if the provider is exposed through command groups or provider choices

## 9. Test expectations

At minimum, add:

- router unit tests for request parsing/building
- router unit tests for response parsing and failure handling
- schema validation tests for `TableSchema` and `DatasetSpec`
- client tests for dataset lookup and pipeline wiring where relevant

Also run the local quality gates expected for provider changes:

```bash
uv run mypy packages/vertex-forager/src --strict
uv run pytest packages/ --cov-fail-under=80 -q
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict
```

## 10. Provider checklist

- package files created under `providers/<name>/`
- `BaseRouter` subclass implemented
- `BaseClient` subclass implemented
- `get_supported_datasets()` implemented
- `get_dataset_spec()` implemented
- schema entries added and registered
- client dispatch updated
- router dispatch updated
- public exports/types updated if needed
- docs updated
- tests added and local gates passing
