from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
import threading
import time
import os

import polars as pl
import psutil
import pytest

from vertex_forager.core.config import EngineConfig, FramePacket, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import BaseWriter, WriteResult


@pytest.mark.asyncio
@pytest.mark.skipif(os.getenv("VF_ENABLE_MEMORY_PEAK_TEST") != "1", reason="memory-peak test disabled by default")
async def test_chunked_flush_lower_memory_peak() -> None:
    async def _run(chunk_rows: int | None) -> tuple[int, int]:
        mock_writer = AsyncMock(spec=BaseWriter)

        async def _write(pkt: FramePacket) -> WriteResult:
            return WriteResult(table=pkt.table, rows=len(pkt.frame))

        mock_writer.write.side_effect = _write
        cfg = EngineConfig(requests_per_minute=100, writer_chunk_rows=chunk_rows)
        vf = VertexForager(
            router=MagicMock(),
            http=MagicMock(),
            writer=mock_writer,
            mapper=MagicMock(),
            config=cfg,
            controller=MagicMock(),
        )
        pkt_q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
        result = RunResult(provider="test")
        frames = [pl.DataFrame({"id": list(range(i * 50_000, (i + 1) * 50_000))}) for i in range(5)]
        for frame in frames:
            pkt_q.put_nowait(
                FramePacket(provider="test", table="t", frame=frame, observed_at=datetime.now())
            )
        pkt_q.put_nowait(None)
        stop = threading.Event()
        peak = {"v": 0}
        proc = psutil.Process()

        def _sampler() -> None:
            local_peak = 0
            while not stop.is_set():
                rss = proc.memory_info().rss
                if rss > local_peak:
                    local_peak = rss
                time.sleep(0.01)
            if peak["v"] < local_peak:
                peak["v"] = local_peak

        t = threading.Thread(target=_sampler, daemon=True)
        t.start()
        await vf._writer_worker(pkt_q=pkt_q, result=result, result_lock=asyncio.Lock())
        stop.set()
        t.join(timeout=1.0)
        return peak["v"], int(mock_writer.write.await_count)

    baseline_peak, baseline_calls = await _run(chunk_rows=None)
    chunked_peak, chunked_calls = await _run(chunk_rows=50_000)
    margin = 5 * 1024 * 1024
    assert chunked_calls > 1
    assert baseline_calls == 1
    assert chunked_peak + margin < baseline_peak
