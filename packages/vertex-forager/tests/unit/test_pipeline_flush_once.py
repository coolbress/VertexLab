from __future__ import annotations

import pytest

from vertex_forager.core.config import EngineConfig
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager


class _StubClient:
    async def aclose(self) -> None:  # pragma: no cover
        return None


class _FlushWriter:
    def __init__(self) -> None:
        self.calls = 0

    async def flush(self) -> None:  # pragma: no cover
        self.calls += 1


@pytest.mark.asyncio
async def test_try_flush_once_consume_false_sets_flag() -> None:
    cfg = EngineConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=_FlushWriter(),  # type: ignore[arg-type]
        mapper=None,
        config=cfg,
        controller=ctrl,
    )
    # First call should invoke writer.flush and set flag
    await eng._try_flush_once(suppress=True, consume=False)  # type: ignore[attr-defined]
    assert getattr(eng, "_writer_flushed", False) is True  # type: ignore[attr-defined]
    # Second call should be a no-op
    await eng._try_flush_once(suppress=True, consume=False)  # type: ignore[attr-defined]


class _RaisingWriter:
    async def flush(self) -> None:  # pragma: no cover
        raise RuntimeError("flush error")


@pytest.mark.asyncio
async def test_try_flush_once_unsuppressed_raises() -> None:
    cfg = EngineConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=_RaisingWriter(),  # type: ignore[arg-type]
        mapper=None,
        config=cfg,
        controller=ctrl,
    )
    with pytest.raises(RuntimeError):
        await eng._try_flush_once(suppress=False, consume=False)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_try_flush_once_suppressed_ignores_errors() -> None:
    cfg = EngineConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=_RaisingWriter(),  # type: ignore[arg-type]
        mapper=None,
        config=cfg,
        controller=ctrl,
    )
    # Should not raise when suppress=True
    await eng._try_flush_once(suppress=True, consume=False)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_try_flush_once_consume_true_no_flag() -> None:
    cfg = EngineConfig(requests_per_minute=60, concurrency=1)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    w = _FlushWriter()
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=w,  # type: ignore[arg-type]
        mapper=None,
        config=cfg,
        controller=ctrl,
    )
    await eng._try_flush_once(suppress=True, consume=True)  # type: ignore[attr-defined]
    assert w.calls >= 1
    # Subsequent call with consume=False should set the flushed flag
    await eng._try_flush_once(suppress=True, consume=False)  # type: ignore[attr-defined]
    assert getattr(eng, "_writer_flushed", False) is True  # type: ignore[attr-defined]
