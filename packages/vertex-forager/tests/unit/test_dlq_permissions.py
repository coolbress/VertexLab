from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from vertex_forager.core.config import EngineConfig, FramePacket
from vertex_forager.core.pipeline import RunResult, VertexForager
from vertex_forager.writers.base import BaseWriter, WriteResult


@pytest.mark.asyncio
async def test_dlq_ipc_file_mode_is_0600(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))

    mock_writer = AsyncMock(spec=BaseWriter)

    async def write_side_effect(pkt: FramePacket) -> WriteResult:
        raise Exception("Disk Full")

    mock_writer.write.side_effect = write_side_effect

    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()

    forager = VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=EngineConfig(requests_per_minute=100),
        controller=mock_controller,
    )

    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()

    pkt_q.put_nowait(
        FramePacket(
            provider="test",
            table="perm_test",
            frame=pl.DataFrame({"id": [1]}),
            observed_at=datetime.now(timezone.utc),
        )
    )
    pkt_q.put_nowait(None)

    await forager._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)

    dlq_dir = tmp_path / "app" / "cache" / "dlq" / "perm_test"
    files = list(dlq_dir.glob("batch_*.ipc"))
    assert files, "DLQ IPC file not found"
    mode = files[0].stat().st_mode & 0o777
    assert mode == 0o600
