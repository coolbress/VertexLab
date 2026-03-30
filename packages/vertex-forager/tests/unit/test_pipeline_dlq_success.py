from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket, ResolvedClientConfig, RunResult
from vertex_forager.core.controller import FlowController
from vertex_forager.core.http import HttpExecutor
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import WriteResult


class _CollectingWriter:
    def __init__(self) -> None:
        self.written: list[FramePacket] = []

    async def write(self, pkt: FramePacket) -> WriteResult:  # pragma: no cover
        self.written.append(pkt)
        return WriteResult(table=pkt.table, rows=len(pkt.frame))

    async def flush(self) -> None:  # pragma: no cover
        return None


class _StubClient:
    async def aclose(self) -> None:  # pragma: no cover
        return None


@pytest.mark.asyncio
async def test_persist_packets_with_dlq_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, concurrency=1, dlq_enabled=True)
    ctrl = FlowController(requests_per_minute=60, concurrency_limit=1)
    writer = _CollectingWriter()
    eng = VertexForager(
        router=None,  # type: ignore[arg-type]
        http=HttpExecutor(client=_StubClient()),
        writer=writer,  # type: ignore[arg-type]
        mapper=None,
        config=cfg,
        controller=ctrl,
    )
    # Make cache dir deterministic
    import vertex_forager.core.pipeline as pipeline_mod

    monkeypatch.setattr(pipeline_mod, "get_cache_dir", lambda: tmp_path, raising=True)
    pkt = FramePacket(provider="p", table="t", frame=pl.DataFrame({"a": [1]}), observed_at=datetime.now(timezone.utc))
    res = RunResult(provider="p")
    lock = asyncio.Lock()
    await eng._persist_packets_with_dlq([pkt], res, lock)
    assert writer.written
    assert writer.written[0].table == "t"
    assert not res.errors
    assert not res.dlq_pending
