from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket, ResolvedClientConfig, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager


class _FailingWriter:
    async def write(self, pkt: FramePacket) -> Any:  # pragma: no cover - invoked to trigger DLQ
        raise RuntimeError("writer blew up")

    async def flush(self) -> None:  # pragma: no cover
        return None


class _StubClient:
    async def aclose(self) -> None:  # pragma: no cover
        return None


@pytest.mark.asyncio
async def test_persist_packets_with_dlq_spool_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Engine with failing writer to force DLQ spool path
    config = ResolvedClientConfig(requests_per_minute=60, concurrency=1)
    controller = FlowController(requests_per_minute=60, concurrency_limit=1)
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=_FailingWriter(),
        mapper=None,
        config=config,
        controller=controller,
    )
    # Force get_cache_dir to point to tmp
    import vertex_forager.core.pipeline as pipeline_mod

    monkeypatch.setattr(pipeline_mod, "get_cache_dir", lambda: tmp_path, raising=True)

    # Create a tiny packet
    pkt = FramePacket(
        provider="x",
        table="t",
        frame=pl.DataFrame({"a": [1]}),
        observed_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    res = RunResult(provider="x")
    lock = asyncio.Lock()

    # Make DLQ spool fail by raising from os.open
    def fake_open(*args: object, **kwargs: object) -> int:
        raise OSError("nope")

    monkeypatch.setattr(os, "open", fake_open)

    await eng._persist_packets_with_dlq([pkt], res, lock)

    # When spool fails, packet should be accounted in dlq_pending and error list updated
    error_messages = "".join([e.message for e in res.errors])
    assert "writer blew up" in error_messages
    assert res.dlq_pending.get("t")
