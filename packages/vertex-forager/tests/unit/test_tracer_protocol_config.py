from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext

from vertex_forager.core.config import EngineConfig


class StubTracer:
    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, object] | None = None,
    ) -> AbstractContextManager[object] | None:
        return nullcontext()


def test_engine_config_accepts_tracer_protocol() -> None:
    cfg = EngineConfig(requests_per_minute=60, tracer=StubTracer(), otel_enabled=True)
    assert cfg.tracer is not None
