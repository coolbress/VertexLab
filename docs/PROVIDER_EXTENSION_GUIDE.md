# Provider Extension Guide

This guide explains how to add a new provider to Vertex Forager following DIP and existing pipeline conventions.

## Components

- Router (IRouter): Generates `FetchJob`s and parses raw payloads into `FramePacket`s.
- Library Fetcher (optional): Implements library calls for non-HTTP sources and registers a scheme.
- Client: Orchestrates pipeline execution using `create_router` and writer factories.

## Router Job Generation

- Use scheme-based URLs for library calls: `mylib://SYMBOL`
- Use `RequestSpec.params["lib"]` to describe the library call:
  - `{"type": "download", "kwargs": {...}}`
  - `{"type": "ticker_attr", "attr": "info", "kwargs": {...}}`
- For HTTP endpoints, use `http(s)://...` as usual.

## Parsing

- Convert raw bytes (pickle for library, bytes for HTTP) into Polars DataFrames.
- Inject provider metadata and standardize columns using routers/transforms.
- Produce `FramePacket`s and optional `next_jobs` for pagination.

## Library Fetcher Pattern

- Implement a fetcher under `vertex_forager/providers/<provider>/fetcher.py`:
  - Subclass `BaseLibraryFetcher`, set `scheme`, implement `fetch(spec)`.
  - Register on import: `register_library_fetcher(MyProviderFetcher())`.
- Ensure package `__init__.py` dynamically imports `fetcher` to guarantee registration.

## DIP Boundaries

- Core depends on abstractions (IRouter, IWriter, IMapper).
- Provider implementations inject concretes via factories and registries.

## Example: YFinance

- Router creates `yfinance://AAPL` and params `{"lib": {"type": "ticker_attr", "attr": "info"}}`.
- Fetcher calls `core.http.yf.Ticker(symbol).info()` and returns result; core pickles it.
- Router parses pickle payload into `FramePacket`.

### Sample Code

- Fetcher: [fetcher.py](../packages/vertex-forager/src/vertex_forager/providers/yfinance/fetcher.py)
- Router job generation: [router.py:generate_jobs](../packages/vertex-forager/src/vertex_forager/providers/yfinance/router.py#L160-L220)
- Router parse example: [router.py:parse](../packages/vertex-forager/src/vertex_forager/providers/yfinance/router.py#L520-L600)

### Checklist

- Define scheme and implement `fetch(spec)` via `BaseLibraryFetcher`.
- Register fetcher on import and ensure package `__init__.py` loads it.
- Generate jobs with scheme URLs and `params["lib"]`.
- Parse payloads to `FramePacket`s; add provider metadata and normalize columns.
- Add tests for fetcher (monkeypatch `core.http.<lib>`) and router parsing.

## Testing

- Unit tests should monkeypatch `vertex_forager.core.http.yf` for library calls.
- Verify pipeline end-to-end with mock writers and routers.
