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

## Minimal HTTP provider skeleton

Use an HTTP router when the provider exposes REST or CSV endpoints and you want the shared executor to handle retries, pacing, and transport concerns.

```python
from __future__ import annotations

from datetime import datetime, timezone
import io
import polars as pl

from vertex_forager.core.config import FetchJob, FramePacket, ParseResult
from vertex_forager.routers.base import BaseRouter
from vertex_forager.routers.errors import raise_quandl_error
from vertex_forager.routers.jobs import single_symbol_job


class ExampleHttpRouter(BaseRouter[str]):
    @property
    def provider(self) -> str:
        return "example"

    async def generate_jobs(self, *, dataset: str, symbols: list[str] | None, **kwargs: object):
        if not symbols:
            return
        for symbol in symbols:
            yield single_symbol_job(
                provider=self.provider,
                dataset=dataset,
                symbol=symbol,
                url=f"https://api.example.com/{dataset}",
                params={"symbol": symbol},
                auth=None,
                context={"symbol": symbol},
            )

    def parse(self, *, job: FetchJob, payload: bytes) -> ParseResult:
        frame = pl.read_csv(io.BytesIO(payload))
        if "error" in frame.columns:
            raise_quandl_error(self.provider, {"message": frame["error"][0]})

        packet = FramePacket(
            provider=self.provider,
            table=f"{self.provider}_{job.dataset}",
            frame=frame,
            observed_at=datetime.now(timezone.utc),
            context=job.context,
        )
        return ParseResult(packets=[packet], next_jobs=[])
```

## What to customize

- `generate_jobs`
  - Build one or more `FetchJob` objects with provider-specific URLs, params, auth, and pagination context.
- `parse`
  - Decode raw bytes into a `polars.DataFrame`, map provider-specific columns, and return `FramePacket` objects.
- Error mapping
  - Convert provider error payloads into standard exceptions through helpers in `routers/errors.py` so client code sees a consistent error surface.

## When to use this approach

- Use an HTTP router for providers that expose normal HTTP endpoints and fit the shared request executor.
- Use the companion [Provider Plugin Guide](provider-plugin.md) instead when the provider requires a Python library or non-HTTP transport.
