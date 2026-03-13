from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import polars as pl
import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

from vertex_forager.core.pipeline import VertexForager, RunResult
from vertex_forager.core.config import FramePacket, EngineConfig
from vertex_forager.writers.base import BaseWriter, WriteResult


@pytest.mark.asyncio
async def test_dlq_disabled_skips_spooling(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))
    mock_writer = AsyncMock(spec=BaseWriter)
    async def write_side_effect(pkt: FramePacket) -> WriteResult:
        raise Exception("Disk Full")
    mock_writer.write.side_effect = write_side_effect
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()
    cfg = EngineConfig(requests_per_minute=60, dlq_enabled=False)
    forager = VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=cfg,
        controller=mock_controller,
    )
    pkt_q: "asyncio.Queue[FramePacket | None]" = asyncio.Queue()
    result = RunResult(provider="t")
    result_lock = asyncio.Lock()
    pkt_q.put_nowait(FramePacket(provider="t", table="t_fail", frame=pl.DataFrame({"id": [1]}), observed_at=datetime.now(timezone.utc)))
    pkt_q.put_nowait(None)
    await forager._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)
    dlq_dir = tmp_path / "app" / "cache" / "dlq" / "t_fail"
    files = list(dlq_dir.glob("batch_*.ipc"))
    assert files == []
    assert any("DLQ=disabled" in e for e in result.errors)
