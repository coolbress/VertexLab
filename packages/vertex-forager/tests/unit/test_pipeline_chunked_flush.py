from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from vertex_forager.constants import WRITER_CHUNK_ROWS
from vertex_forager.core.config import FramePacket, ResolvedClientConfig, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import BaseWriter, WriteResult


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

    cfg = ResolvedClientConfig(requests_per_minute=100)
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

    # WRITER_CHUNK_ROWS + 1 rows total -> chunks: WRITER_CHUNK_ROWS, 1
    frames = [
        pl.DataFrame({"id": list(range(12_000))}),
        pl.DataFrame({"id": list(range(12_000, WRITER_CHUNK_ROWS))}),
        pl.DataFrame({"id": [WRITER_CHUNK_ROWS]}),
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

    # Expect 2 writes due to chunking, and verify call order/rows explicitly
    calls = list(mock_writer.write.await_args_list)
    assert len(calls) == 2
    first_rows = len(calls[0].args[0].frame)
    second_rows = len(calls[1].args[0].frame)
    # Streaming aggregator fills the first chunk to the limit, then writes the tail
    assert first_rows == WRITER_CHUNK_ROWS
    assert second_rows == 1
    assert result.tables.get("chunk_table", 0) == WRITER_CHUNK_ROWS + 1
    # Verify per-chunk contract: metrics histogram records per-chunk rows in order
    hist = list(forager._hists.get("writer_rows.chunk_table", []))  # type: ignore[attr-defined]
    assert len(hist) == 2
    assert int(hist[0]) == first_rows
    assert int(hist[1]) == second_rows


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
    cfg = ResolvedClientConfig(requests_per_minute=100)
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

    # First chunk WRITER_CHUNK_ROWS rows, then remaining 2 rows to trigger failure on second write
    frames = [
        pl.DataFrame({"id": list(range(WRITER_CHUNK_ROWS))}),
        pl.DataFrame({"id": [WRITER_CHUNK_ROWS]}),
        pl.DataFrame({"id": [WRITER_CHUNK_ROWS + 1]}),
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

    # Exactly one error recorded; rows from the first successful chunk counted
    assert len(result.errors) == 1
    assert result.tables.get("chunk_table", 0) == WRITER_CHUNK_ROWS
    # DLQ spooled file has two failed rows; error summary records remaining=2
    dlq_dir = tmp_path / "app" / "cache" / "dlq" / "chunk_table"
    assert dlq_dir.exists()
    files = sorted(dlq_dir.glob("batch_*.ipc"))
    assert len(files) >= 1
    df = pl.read_ipc(files[0])
    assert df.shape[0] == 2
    assert set(df.get_column("id").to_list()) == {WRITER_CHUNK_ROWS, WRITER_CHUNK_ROWS + 1}
    assert any("DLQ=spooled" in e.message and "remaining=2" in e.message for e in result.errors)


def test_compute_summary_percentiles_and_counters() -> None:
    mock_writer = AsyncMock(spec=BaseWriter)
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()
    cfg = ResolvedClientConfig(requests_per_minute=60)
    vf = VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=cfg,
        controller=mock_controller,
    )
    # Populate histograms/counters
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
