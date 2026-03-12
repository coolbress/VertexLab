from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import MagicMock
import threading
import time
import os
import multiprocessing as mp

import polars as pl
import psutil
import pytest

from vertex_forager.core.config import EngineConfig, FramePacket, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import WriteResult


def _child_run_memory_peak(chunk_rows: int, conn) -> None:
    import asyncio
    from vertex_forager.core.config import FramePacket
    from vertex_forager.writers.base import WriteResult

    class FakeWriter:
        def __init__(self) -> None:
            self.count = 0
        async def write(self, pkt: FramePacket) -> WriteResult:
            self.count += 1
            return WriteResult(table=pkt.table, rows=len(pkt.frame))

    async def _run() -> tuple[int, int]:
        writer = FakeWriter()
        cfg = EngineConfig(requests_per_minute=100, writer_chunk_rows=chunk_rows if chunk_rows > 0 else None)
        vf = VertexForager(
            router=MagicMock(),
            http=MagicMock(),
            writer=writer,
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
        return peak["v"], int(writer.count)

    res = asyncio.run(_run())
    conn.send(res)
    conn.close()

@pytest.mark.asyncio
@pytest.mark.skipif(os.getenv("VF_ENABLE_MEMORY_PEAK_TEST") != "1", reason="memory-peak test disabled by default")
async def test_chunked_flush_lower_memory_peak() -> None:
    ctx = mp.get_context("spawn")
    b_parent, b_child = ctx.Pipe(duplex=False)
    c_parent, c_child = ctx.Pipe(duplex=False)
    p_base = ctx.Process(target=_child_run_memory_peak, args=(0, b_child))
    p_chunk = ctx.Process(target=_child_run_memory_peak, args=(50_000, c_child))
    p_base.start()
    baseline_peak, baseline_calls = b_parent.recv()
    p_base.join(timeout=10)
    assert p_base.exitcode == 0
    p_chunk.start()
    chunked_peak, chunked_calls = c_parent.recv()
    p_chunk.join(timeout=10)
    assert p_chunk.exitcode == 0
    margin = 5 * 1024 * 1024
    assert chunked_calls > 1
    assert baseline_calls == 1
    assert chunked_peak + margin < baseline_peak
