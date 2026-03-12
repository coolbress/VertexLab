from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from vertex_forager.core.config import EngineConfig, FramePacket, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import BaseWriter, WriteResult
from vertex_forager.exceptions import VertexForagerError
from collections import deque


@pytest.mark.asyncio
async def test_chunked_flush_writes_multiple_chunks() -> None:
    # Writer mock that returns rows count
    mock_writer = AsyncMock(spec=BaseWriter)

    async def _write(pkt: FramePacket) -> WriteResult:
        return WriteResult(table=pkt.table, rows=len(pkt.frame))

    mock_writer.write.side_effect = _write

    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()

    cfg = EngineConfig(requests_per_minute=100, writer_chunk_rows=10_000)
    forager = VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=cfg,
        controller=mock_controller,
    )

    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()

    # 10001 rows total -> chunks: 10000, 1
    frames = [
        pl.DataFrame({"id": list(range(6000))}),
        pl.DataFrame({"id": list(range(6000, 10000))}),
        pl.DataFrame({"id": [10000]}),
    ]
    for frame in frames:
        pkt_q.put_nowait(
            FramePacket(
                provider="test",
                table="chunk_table",
                frame=frame,
                observed_at=datetime.now(),
            )
        )
    pkt_q.put_nowait(None)

    await forager._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)

    # Expect 2 writes due to chunking
    assert mock_writer.write.await_count == 2
    assert result.tables.get("chunk_table", 0) == 10001


def test_engine_config_writer_chunk_rows_coercion() -> None:
    # writer_chunk_rows as string should coerce to int in assert_valid
    cfg = EngineConfig(requests_per_minute=60, writer_chunk_rows="20000")  # type: ignore[arg-type]
    cfg.assert_valid()
    assert isinstance(cfg.writer_chunk_rows, int)
    assert cfg.writer_chunk_rows == 20000


@pytest.mark.asyncio
async def test_chunked_flush_partial_error(tmp_path, monkeypatch) -> None:
    # Writer mock that fails on second chunk
    call = {"n": 0}
    async def _write(pkt: FramePacket) -> WriteResult:
        call["n"] += 1
        if call["n"] == 1:
            return WriteResult(table=pkt.table, rows=len(pkt.frame))
        raise Exception("Disk Full")

    mock_writer = AsyncMock(spec=BaseWriter)
    mock_writer.write.side_effect = _write

    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()

    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))
    cfg = EngineConfig(requests_per_minute=100, writer_chunk_rows=10_000)
    forager = VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=cfg,
        controller=mock_controller,
    )

    pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()

    # First chunk exact 10000 rows, then remaining 2 rows to trigger failure on second write
    frames = [
        pl.DataFrame({"id": list(range(10000))}),
        pl.DataFrame({"id": [10000]}),
        pl.DataFrame({"id": [10001]}),
    ]
    for frame in frames:
        pkt_q.put_nowait(
            FramePacket(
                provider="test",
                table="chunk_table",
                frame=frame,
                observed_at=datetime.now(),
            )
        )
    pkt_q.put_nowait(None)

    await forager._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)

    # Exactly one error should be recorded; rows from the first successful chunk counted
    assert len(result.errors) == 1
    assert result.tables.get("chunk_table", 0) == 10000


def test_engine_config_writer_chunk_rows_lower_bound() -> None:
    cfg = EngineConfig(requests_per_minute=60, writer_chunk_rows=9_999)
    with pytest.raises(ValueError):
        cfg.assert_valid()


def test_compute_summary_percentiles_and_counters() -> None:
    mock_writer = AsyncMock(spec=BaseWriter)
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()
    cfg = EngineConfig(requests_per_minute=60)
    vf = VertexForager(
        router=mock_router, http=mock_http, writer=mock_writer, mapper=mock_mapper, config=cfg, controller=mock_controller
    )
    # Enable metrics and populate histograms/counters
    vf._metrics_enabled = True  # type: ignore[attr-defined]
    vf._hists = {  # type: ignore[attr-defined]
        "fetch_duration_s": deque([0.1, 0.2, 0.3]),
        "writer_flush_duration_s.tableA": deque([1.0, 2.0, 3.0]),
        "writer_rows.tableA": deque([100.0, 200.0, 300.0]),
    }
    vf._counters = {  # type: ignore[attr-defined]
        "rows_written_total": 600,
        "dlq_spooled_files_total": 2,
        "dlq_rescued_total": 5,
    }
    summary = vf._compute_summary()  # type: ignore[attr-defined]
    assert "fetch_duration_s_p95" in summary
    assert "writer_flush_duration_s.tableA_p50" in summary
    assert summary.get("rows_written_total") == 600.0
    assert summary.get("dlq_spooled_files_total") == 2.0
