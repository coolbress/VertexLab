from __future__ import annotations

import asyncio
from contextlib import AbstractContextManager
from datetime import datetime
import multiprocessing as mp
from multiprocessing.connection import Connection
import os
import threading
import time

import polars as pl
import psutil
import pytest

from vertex_forager.core.domain import FramePacket, RunResult
from vertex_forager.core.writerflush import FlushExecutor, FlushPlanner, FlushRecovery, MetricKind, flush_chunked_table
from vertex_forager.exceptions import RunError
from vertex_forager.writers.base import WriteResult

pytestmark = pytest.mark.manual


class FakeWriter:
    def __init__(self) -> None:
        self.count = 0

    async def write(self, pkt: FramePacket) -> WriteResult:
        self.count += 1
        return WriteResult(table=pkt.table, rows=len(pkt.frame))


class _NoopLogger:
    def debug(self, *args: object, **kwargs: object) -> None:
        return None

    def warning(self, *args: object, **kwargs: object) -> None:
        return None

    def error(self, *args: object, **kwargs: object) -> None:
        return None

    def exception(self, *args: object, **kwargs: object) -> None:
        return None


class _Span:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _NoopObserver:
    def on_metric(self, name: str, value: float, *, kind: MetricKind) -> None:
        del name, value, kind

    def on_log(self, **kwargs: object) -> None:
        del kwargs

    def span(self, name: str, **attributes: object) -> AbstractContextManager[object]:
        del name, attributes
        return _Span()


async def _noop_async() -> None:
    return None


async def _noop_status() -> dict[str, object]:
    return {"status": "spooled", "rescued": 0, "remaining": 0, "path": None, "error": None}


def _build_packets() -> tuple[list[FramePacket], int]:
    rows_per_frame = 100_000
    num_frames = 6
    packets: list[FramePacket] = []
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
        packets.append(FramePacket(provider="test", table="t", frame=frame, observed_at=datetime.now()))
    return packets, rows_per_frame * num_frames


def _build_flush_components(
    writer: FakeWriter,
) -> tuple[_NoopObserver, _NoopLogger, FlushPlanner, FlushExecutor, FlushRecovery]:
    observer = _NoopObserver()
    logger = _NoopLogger()
    planner = FlushPlanner()
    executor = FlushExecutor(
        writer=writer,
        observer=observer,
        concat_frames_with_flex=lambda **kwargs: pl.concat(kwargs["frames"], how="vertical"),
        validate_unique_key=lambda **kwargs: None,
        validate_data_quality=lambda **kwargs: _noop_async(),
    )
    recovery = FlushRecovery(
        writer=writer,
        config=object(),
        observer=observer,
        spool_to_dlq_and_rescue=lambda **kwargs: _noop_status(),
        build_writer_error_summary=lambda **kwargs: RunError.from_exception(
            exc=kwargs["exc"], provider="test", dataset="t", symbol=""
        ),
        compute_error_cls=Exception,
        validation_error_cls=Exception,
        primary_key_missing_error_cls=Exception,
        primary_key_null_error_cls=Exception,
        dlq_spool_error_cls=Exception,
        duckdb_module=None,
        logger=logger,
    )
    return observer, logger, planner, executor, recovery


def _sample_peak(stop: threading.Event, peak: dict[str, int]) -> None:
    proc = psutil.Process()
    local_peak = 0
    while not stop.is_set():
        rss = proc.memory_info().rss
        if rss > local_peak:
            local_peak = rss
        time.sleep(0.01)
    if peak["v"] < local_peak:
        peak["v"] = local_peak


def _child_run_memory_peak(chunk_rows: int, conn: Connection) -> None:
    async def _run() -> tuple[int, int]:
        writer = FakeWriter()
        packets, total_rows = _build_packets()
        flush_size = chunk_rows if chunk_rows > 0 else total_rows + 1
        observer, logger, planner, executor, recovery = _build_flush_components(writer)
        result = RunResult(provider="test")
        stop = threading.Event()
        peak = {"v": 0}
        t = threading.Thread(target=_sample_peak, args=(stop, peak), daemon=True)
        t.start()
        await flush_chunked_table(
            table="t",
            packets=packets,
            schema=None,
            chunk_size=flush_size,
            total_rows=total_rows,
            buffers={"t": packets.copy()},
            buffer_rows={"t": total_rows},
            result=result,
            result_lock=asyncio.Lock(),
            planner=planner,
            executor=executor,
            recovery=recovery,
            observer=observer,
            logger=logger,
        )
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
    b_child.close()
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
    dynamic_margin = max(int(baseline_peak * 0.05), 10 * 1024 * 1024)
    assert chunked_calls > 1
    assert baseline_calls == 1
    assert chunked_peak + dynamic_margin < baseline_peak
