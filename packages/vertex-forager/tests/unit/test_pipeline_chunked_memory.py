from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import threading
import time
from datetime import datetime
from multiprocessing.connection import Connection
from unittest.mock import MagicMock

import polars as pl
import psutil
import pytest
from vertex_forager.core.config import EngineConfig, FramePacket, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.writers.base import WriteResult


def _child_run_memory_peak(chunk_rows: int, conn: Connection) -> None:
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
        # Build a wider, larger payload to amplify memory differences
        rows_per_frame = 100_000
        num_frames = 6
        frames: list[pl.DataFrame] = []
        for i in range(num_frames):
            start = i * rows_per_frame
            end = (i + 1) * rows_per_frame
            base_df = pl.DataFrame({"c0": pl.arange(start, end, eager=True)})
            frame = base_df.with_columns(
                (pl.col("c0") + 1).alias("c1"),
                (pl.col("c0") * 2).alias("c2"),
                (pl.col("c0") ^ pl.lit(0xAAAA)).alias("c3"),
                (pl.col("c0") // 3).alias("c4"),
                (pl.col("c0") % 97).alias("c5"),
                (pl.col("c0") * 3).alias("c6"),
                (pl.col("c0") - 1).alias("c7"),
            )
            frames.append(frame)
        total_rows = rows_per_frame * num_frames
        # Ensure baseline vs chunked behavior differs ONLY by writer_chunk_rows,
        # not by early threshold flushes
        cfg = EngineConfig(
            requests_per_minute=100,
            writer_chunk_rows=chunk_rows if chunk_rows > 0 else None,
            flush_threshold_rows=total_rows + 1,
        )
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
        for frame in frames:
            pkt_q.put_nowait(
                FramePacket(
                    provider="test", table="t", frame=frame, observed_at=datetime.now()
                )
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

@pytest.mark.skipif(
    os.getenv("VF_ENABLE_MEMORY_PEAK_TEST") != "1",
    reason="memory-peak test disabled by default",
)
def test_chunked_flush_lower_memory_peak() -> None:
    ctx = mp.get_context("spawn")
    b_parent, b_child = ctx.Pipe(duplex=False)
    c_parent, c_child = ctx.Pipe(duplex=False)
    p_base = ctx.Process(target=_child_run_memory_peak, args=(0, b_child))
    p_chunk = ctx.Process(target=_child_run_memory_peak, args=(50_000, c_child))
    p_base.start()
    # Close child ends in parent immediately after start to avoid descriptor leaks
    b_child.close()
    # Ensure child exited successfully before receiving to avoid blocking on crash
    p_base.join(timeout=10)
    assert p_base.exitcode == 0, f"p_base failed with exit code {p_base.exitcode}"
    baseline_peak, baseline_calls = b_parent.recv()
    b_parent.close()
    p_chunk.start()
    c_child.close()
    p_chunk.join(timeout=10)
    assert p_chunk.exitcode == 0, f"p_chunk failed with exit code {p_chunk.exitcode}"
    chunked_peak, chunked_calls = c_parent.recv()
    c_parent.close()
    # Dynamic margin: require ≥5% drop or 10 MiB, whichever is larger.
    dynamic_margin = max(int(baseline_peak * 0.05), 10 * 1024 * 1024)
    assert chunked_calls > 1
    assert baseline_calls == 1
    assert chunked_peak + dynamic_margin < baseline_peak
