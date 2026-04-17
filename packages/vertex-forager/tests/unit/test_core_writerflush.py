from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime

import polars as pl
import pytest

from vertex_forager.constants import WRITER_CHUNK_ROWS
from vertex_forager.core.config import FramePacket, RunResult
from vertex_forager.core.writerflush import (
    FlushChunk,
    FlushExecutor,
    FlushPlanner,
    FlushRecovery,
    buffer_or_flush_packet,
    flush_writer_table,
    validate_unique_key,
    writer_table_context,
    writer_worker,
)
from vertex_forager.exceptions import PrimaryKeyMissingError, RunError
from vertex_forager.schema.config import TableSchema


def _packet(table: str, rows: int = 1) -> FramePacket:
    return FramePacket(
        provider="sharadar",
        table=table,
        frame=pl.DataFrame({"x": list(range(rows))}),
        observed_at=datetime.now(),
    )


class _Observer:
    def __init__(self) -> None:
        self.metrics: list[tuple[str, float, str]] = []
        self.logs: list[dict[str, object]] = []

    def on_metric(self, name: str, value: float, *, kind: str) -> None:
        self.metrics.append((name, value, kind))

    def on_log(self, **kwargs: object) -> None:
        self.logs.append(dict(kwargs))

    @contextmanager
    def span(self, name: str, **attributes: object) -> object:
        del name, attributes
        yield


class _Writer:
    def __init__(self) -> None:
        self.packets: list[FramePacket] = []

    async def write(self, packet: FramePacket) -> object:
        self.packets.append(packet)
        return type("WriteResult", (), {"rows": len(packet.frame), "table": packet.table})()


class _ValidationError(Exception):
    pass


@pytest.mark.asyncio
async def test_buffer_or_flush_packet_triggers_flush_at_threshold() -> None:
    buffers: dict[str, list[FramePacket]] = defaultdict(list)
    buffer_rows: dict[str, int] = defaultdict(int)
    called: list[str] = []

    async def _flush_writer_table(**kwargs: object) -> None:
        called.append(kwargs["table"])

    await buffer_or_flush_packet(
        packet=_packet("t", rows=2),
        threshold=2,
        buffers=buffers,
        buffer_rows=buffer_rows,
        result=RunResult(provider="sharadar"),
        result_lock=asyncio.Lock(),
        flush_writer_table=_flush_writer_table,
        progress_log_chunk_rows=1,
        logger=type("L", (), {"debug": lambda *args, **kwargs: None})(),
    )
    assert called == ["t"]


@pytest.mark.asyncio
async def test_writer_worker_flushes_on_shutdown_sentinel() -> None:
    q: asyncio.Queue[FramePacket | None] = asyncio.Queue()
    await q.put(None)
    called = {"flush_all": 0}

    async def _flush_all_writer_buffers(**kwargs: object) -> None:
        called["flush_all"] += 1

    async def _buffer_or_flush_packet(**kwargs: object) -> None:
        return None

    async def _flush_on_writer_cancel(**kwargs: object) -> None:
        return None

    await writer_worker(
        pkt_q=q,
        result=RunResult(provider="sharadar"),
        result_lock=asyncio.Lock(),
        flush_threshold=10,
        flush_all_writer_buffers=_flush_all_writer_buffers,
        buffer_or_flush_packet=_buffer_or_flush_packet,
        flush_on_writer_cancel=_flush_on_writer_cancel,
        dlq_spool_error_cls=Exception,
        logger=type("L", (), {"debug": lambda *args, **kwargs: None, "exception": lambda *args, **kwargs: None})(),
    )
    assert called["flush_all"] == 1


def test_writer_table_context_prefers_symbol_then_ticker() -> None:
    pkt = FramePacket(
        provider="sharadar",
        table="t",
        frame=pl.DataFrame({"x": [1]}),
        observed_at=datetime.now(),
        context={"ticker": "AAPL", "symbol": "MSFT"},
    )
    provider, symbol = writer_table_context([pkt])
    assert provider == "sharadar"
    assert symbol == "MSFT"


def test_validate_unique_key_raises_missing_column() -> None:
    schema = TableSchema(
        table="t",
        schema={"ticker": pl.String},
        unique_key=("ticker",),
    )

    with pytest.raises(PrimaryKeyMissingError):
        validate_unique_key(schema=schema, table="t", frame=pl.DataFrame({"x": [1]}))


def test_flush_planner_splits_packets_by_chunk_rows() -> None:
    planner = FlushPlanner()
    chunks = planner.plan(packets=[_packet("t", 2), _packet("t", 2), _packet("t", 3)], chunk_size=4)

    assert [(chunk.index, chunk.start_index, chunk.rows, len(chunk.packets)) for chunk in chunks] == [
        (0, 0, 4, 2),
        (1, 2, 3, 1),
    ]


@pytest.mark.asyncio
async def test_flush_executor_validates_and_writes_chunk() -> None:
    observer = _Observer()
    writer = _Writer()
    calls = {"quality": 0, "unique": 0}

    async def _validate_quality(**kwargs: object) -> None:
        calls["quality"] += 1
        assert kwargs["table"] == "t"

    def _validate_unique(**kwargs: object) -> None:
        calls["unique"] += 1
        assert kwargs["table"] == "t"

    executor = FlushExecutor(
        writer=writer,
        observer=observer,
        concat_frames_with_flex=lambda **kwargs: pl.concat(kwargs["frames"], how="vertical"),
        validate_unique_key=_validate_unique,
        validate_data_quality=_validate_quality,
    )
    packets = [_packet("t", 2), _packet("t", 1)]
    chunk = FlushChunk(index=0, start_index=0, packets=packets, frames=[packet.frame for packet in packets], rows=3)
    result = RunResult(provider="sharadar")

    await executor.execute(
        table="t",
        chunk=chunk,
        schema=TableSchema(table="t", schema={"x": pl.Int64}),
        result=result,
        result_lock=asyncio.Lock(),
    )

    assert calls == {"quality": 1, "unique": 1}
    assert len(writer.packets) == 1
    assert len(writer.packets[0].frame) == 3
    assert result.tables == {"t": 3}
    assert any(metric[0] == "writer_flushes" for metric in observer.metrics)
    assert observer.logs[0]["stage"] == "write_flush_chunk_1"


@pytest.mark.asyncio
async def test_flush_recovery_handles_validation_errors_with_writererror_prefix() -> None:
    observer = _Observer()
    build_calls: list[dict[str, object]] = []
    spool_calls: list[dict[str, object]] = []

    async def _spool(**kwargs: object) -> object:
        spool_calls.append(dict(kwargs))
        return {"status": "spooled", "rescued": 0, "remaining": len(kwargs["packets"]), "path": "x", "error": None}

    def _build_summary(**kwargs: object) -> RunError:
        build_calls.append(dict(kwargs))
        return RunError.from_exception(exc=kwargs["exc"], provider="sharadar", dataset="t", symbol="")

    recovery = FlushRecovery(
        writer=_Writer(),
        config=object(),
        observer=observer,
        spool_to_dlq_and_rescue=_spool,
        build_writer_error_summary=_build_summary,
        compute_error_cls=Exception,
        validation_error_cls=_ValidationError,
        primary_key_missing_error_cls=PrimaryKeyMissingError,
        primary_key_null_error_cls=Exception,
        dlq_spool_error_cls=Exception,
        duckdb_module=None,
        logger=type("L", (), {"error": lambda *args, **kwargs: None, "exception": lambda *args, **kwargs: None})(),
    )
    packets = [_packet("t", 1), _packet("t", 1), _packet("t", 1)]
    chunk = FlushChunk(index=1, start_index=1, packets=packets[1:], frames=[p.frame for p in packets[1:]], rows=2)
    result = RunResult(provider="sharadar")
    buffers = {"t": packets.copy()}
    buffer_rows = {"t": 3}

    await recovery.recover(
        table="t",
        chunk=chunk,
        packets=packets,
        exc=_ValidationError("bad"),
        buffers=buffers,
        buffer_rows=buffer_rows,
        result=result,
        result_lock=asyncio.Lock(),
    )

    assert build_calls[0]["prefix"] == "WriterError"
    assert len(spool_calls[0]["packets"]) == 2
    assert len(result.errors) == 1
    assert buffers["t"] == []
    assert buffer_rows["t"] == 0


@pytest.mark.asyncio
async def test_flush_writer_table_uses_chunked() -> None:
    buffers: dict[str, list[FramePacket]] = {"t": [_packet("t", rows=1)]}
    buffer_rows: dict[str, int] = {"t": 1}
    called = {"chunked": 0, "chunk_size": None}

    async def _flush_chunked_table(**kwargs: object) -> None:
        called["chunked"] += 1
        called["chunk_size"] = kwargs["chunk_size"]

    await flush_writer_table(
        table="t",
        buffers=buffers,
        buffer_rows=buffer_rows,
        result=RunResult(provider="sharadar"),
        result_lock=asyncio.Lock(),
        get_table_schema=lambda _table: TableSchema(table="t", schema={"x": pl.Int64}),
        flush_chunked_table=_flush_chunked_table,
    )
    assert called["chunked"] == 1
    assert called["chunk_size"] == WRITER_CHUNK_ROWS
