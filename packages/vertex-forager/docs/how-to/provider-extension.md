# Provider Extension Guide

Guidelines for extending Vertex Forager with new providers.

See also: [Provider Plugin Guide](provider-plugin.md) for non‑HTTP plugins.

## Topics

- Router contracts and responsibilities
- Transform helpers
- Job builders and pagination context
- Error mapping to standard exceptions

## Steps

1) Decide transport (HTTP vs Library).
2) Implement router interfaces:
   - `generate_jobs` to yield a sequence/async-iterator of `FetchJob`
   - `parse` to convert responses into normalized frames
3) Use `routers/transforms.py` for common operations (dates, empties, columns).
4) Construct jobs with `routers/jobs.py` helpers; use `make_pagination_context` for cursor APIs.
5) Map provider‑specific exceptions via `routers/errors.py`.
6) Add tests (unit + integration) and run quality gates (ruff, mypy, pytest).
