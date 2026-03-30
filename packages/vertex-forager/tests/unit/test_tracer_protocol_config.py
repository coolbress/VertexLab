from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext

from vertex_forager.core.config import AdvancedConfig, ResolvedClientConfig


class StubTracer:
    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, object] | None = None,
    ) -> AbstractContextManager[object] | None:
        return nullcontext()


def test_resolved_client_config_accepts_tracer_protocol() -> None:
    cfg = ResolvedClientConfig(
        requests_per_minute=60,
        advanced=AdvancedConfig(tracer=StubTracer(), otel_enabled=True),
    )
    assert cfg.advanced.tracer is not None
