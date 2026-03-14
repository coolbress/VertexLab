from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
import polars as pl
import pytest

from vertex_forager.core.config import EngineConfig, FramePacket, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import BaseWriter, WriteResult


@pytest.mark.asyncio
async def test_flexible_schema_add_column_evolution(monkeypatch) -> None:
    mock_writer = AsyncMock(spec=BaseWriter)
    captured = {"frames": []}
    async def _write(pkt: FramePacket) -> WriteResult:
        captured["frames"].append(pkt.frame)
        return WriteResult(table=pkt.table, rows=len(pkt.frame))
    mock_writer.write.side_effect = _write
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()
    cfg = EngineConfig(requests_per_minute=60, writer_chunk_rows=None)
    vf = VertexForager(router=mock_router, http=mock_http, writer=mock_writer, mapper=mock_mapper, config=cfg, controller=mock_controller)
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="yfinance")
    result_lock = asyncio.Lock()
    pkt_q.put_nowait(FramePacket(provider="yfinance", table="yfinance_fast_info", frame=pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "fetched_at": [datetime.now(timezone.utc)]}), observed_at=datetime.now(timezone.utc)))
    pkt_q.put_nowait(FramePacket(provider="yfinance", table="yfinance_fast_info", frame=pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"], "extra_col": ["x"], "fetched_at": [datetime.now(timezone.utc)]}), observed_at=datetime.now(timezone.utc)))
    pkt_q.put_nowait(None)
    await vf._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)
    assert captured["frames"], "writer.write was not called"
    cols = set(captured["frames"][0].columns)
    assert {"provider", "ticker", "fetched_at"}.issubset(cols)
    assert "extra_col" in cols


@pytest.mark.asyncio
async def test_pk_missing_raises_and_spools(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))
    mock_writer = AsyncMock(spec=BaseWriter)
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()
    cfg = EngineConfig(requests_per_minute=60)
    vf = VertexForager(router=mock_router, http=mock_http, writer=mock_writer, mapper=mock_mapper, config=cfg, controller=mock_controller)
    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="yfinance")
    result_lock = asyncio.Lock()
    pkt_q.put_nowait(FramePacket(provider="yfinance", table="yfinance_price", frame=pl.DataFrame({"provider": ["yfinance"], "ticker": ["AAPL"]}), observed_at=datetime.now(timezone.utc)))
    pkt_q.put_nowait(None)
    await vf._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)
    assert any("Missing PK column" in e for e in result.errors)
