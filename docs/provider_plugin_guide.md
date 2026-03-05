# Provider Plugin Guide

Purpose: Define a clear, provider‑agnostic extension model for non‑HTTP providers while keeping transport concerns centralized and shared across the project.

## Design Goals
- Provider‑agnostic core: No provider‑specific branches in core/http; shared timeout, retries, rate limiting, logging, and error translation.
- Unified interface: Routers/clients call fetch(spec) regardless of transport; HTTP stays in core/http, non‑HTTP uses provider plugins.
- Safety & testability: Strict invocation rules, clear error contracts, and comprehensive tests.

## When to Implement a Fetcher
- HTTP providers (e.g., Sharadar) do NOT require a fetcher. Use router‑built RequestSpec with http/https URLs; core/http executes requests.
- Library / non‑HTTP providers (e.g., yfinance) MUST provide a fetcher. The fetcher wraps provider APIs behind a scheme (e.g., yfinance://) and implements fetch(spec) with explicit, provider‑specific logic.

## Contract (Interfaces & Types)
- RequestSpec (core/config.py)
  - url: string; for library providers use "<scheme>://<payload>" (e.g., "yfinance://AAPL")
  - params: dict including "dataset" and "lib" describing the library call
  - headers/json_body/data/auth/timeout_s follow the shared RequestSpec shape
- BaseLibraryFetcher (core/library.py)
  - scheme: unique string identifying the provider plugin (e.g., "yfinance")
  - fetch(spec: RequestSpec) -> Any: executes the provider‑specific call
  - parse_spec(spec) -> (payload: str, dataset: str, lib: dict): validates scheme and call structure
- Core Dispatch (core/http.py)
  - HttpExecutor.fetch(spec) routes non‑HTTP URLs to the registry and returns pickled Python objects; HTTP returns bytes

## Security & Safety (Invocation Rules)
- Allowed calls only:
  - Public methods/attributes; block private/dunder names (startswith("_") or "__" in name)
  - Validate lib dict: type (e.g., "download", "ticker_attr"), attr name, kwargs types (JSONValue only)
- No leakage to core/http:
  - Never import provider modules inside core/http; core/http uses get_library_fetcher(scheme) only
- Logging:
  - Structured logs with provider/dataset/symbol context; do not log secrets; mask or omit tokens
- Errors:
  - Raise ValueError for invalid spec/unsupported call from fetcher; core/http maps/propagates as project‑standard

## Registry (Idempotent & Unique)
- register_library_fetcher(fetcher):
  - Enforces unique scheme; raises on duplicate
- get_library_fetcher(scheme):
  - Returns plugin instance or None if not registered
- Registration pattern:
  - Inside providers/<name>/fetcher.py, declare class and call register_library_fetcher(...) on import

## Response Normalization
- Library fetch path returns pickled objects; routers decode and normalize (records/frames, strict types, null policy)
- HTTP path returns raw bytes; routers parse JSON/CSV and normalize similarly
- Ensure downstream routers remain transport‑agnostic: they only decode/normalize payloads

## Testing Requirements
- Unit tests (plugins):
  - parse_spec validation (missing/invalid lib, wrong scheme)
  - safe invocation (reject private/dunder)
  - registry idempotency and uniqueness
- Integration tests (core dispatch):
  - HttpExecutor.fetch routes scheme to plugin; payload is pickled; routers decode and map correctly
- CI:
  - mypy (strict flags in vertex_forager scope), ruff, pytest must pass

## Example: Library Provider (yfinance)
Implementation sketch (simplified):
```python
from typing import Any
from vertex_forager.core.config import RequestSpec
from vertex_forager.core.types import JSONValue
from vertex_forager.core.library import BaseLibraryFetcher, register_library_fetcher
import vertex_forager.core.http as _http_mod

class YFinanceLibraryFetcher(BaseLibraryFetcher):
    scheme = "yfinance"
    def fetch(self, spec: RequestSpec) -> Any:
        payload, dataset, lib = self.parse_spec(spec)
        call_type = lib.get("type")
        kw = lib.get("kwargs")
        call_kwargs: dict[str, JSONValue] = dict(kw) if isinstance(kw, dict) else {}
        if call_type == "download":
            return _http_mod.yf.download(tickers=payload, **call_kwargs)
        if call_type == "ticker_attr":
            attr_name = lib.get("attr")
            if not isinstance(attr_name, str) or attr_name.startswith("_") or "__" in attr_name:
                raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}")
            ticker = _http_mod.yf.Ticker(payload)
            try:
                attr = getattr(ticker, attr_name)
            except AttributeError:
                raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}")
            return attr(**call_kwargs) if callable(attr) else attr
        raise ValueError(f"Unsupported library call type: {call_type}")

register_library_fetcher(YFinanceLibraryFetcher())
```
Router usage:
```python
url = f"yfinance://{symbol}"
params = {"dataset": dataset, "lib": {"type": "ticker_attr", "attr": "info", "kwargs": {}}}
spec = RequestSpec(url=url, params=params)
```

## Adding a New Provider
1. Decide transport:
   - HTTP: no plugin; implement router rules and build http/https RequestSpec
   - Library/non‑HTTP: implement fetcher.py with unique scheme and explicit fetch(spec)
2. Implement normalization in the router; keep transport‑agnostic behavior
3. Register plugin on import; avoid duplicate schemes
4. Add unit/integration tests:
   - Registry and parse_spec
   - Core dispatch via HttpExecutor
   - Router decode/normalize invariants
5. Run CI: ruff, mypy, pytest must be green

## Notes
- Keep provider‑specific logic in providers/<name>/fetcher.py and router.py; avoid core modifications except for shared transport behavior.
- Scheme naming should be short, lowercase, and unique (e.g., "yfinance", "myprovider").
