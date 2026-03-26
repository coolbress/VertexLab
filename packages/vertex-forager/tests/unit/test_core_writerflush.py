from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket, RunResult
from vertex_forager.core.writerflush import (
    buffer_or_flush_packet,
    flush_writer_table,
    validate_unique_key,
    writer_table_context,
    writer_worker,
)
from vertex_forager.exceptions import PrimaryKeyMissingError


def _packet(table: str, rows: int = 1) -> FramePacket:
    return FramePacket(
        provider="sharadar",
        table=table,
        frame=pl.DataFrame({"x": list(range(rows))}),
        observed_at=datetime.now(),
    )


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
    class _Schema:
        def __init__(self) -> None:
            self.unique_key = ["ticker"]

    with pytest.raises(PrimaryKeyMissingError):
        validate_unique_key(schema=_Schema(), table="t", frame=pl.DataFrame({"x": [1]}))


@pytest.mark.asyncio
async def test_flush_writer_table_uses_legacy_when_chunk_size_missing() -> None:
    buffers: dict[str, list[FramePacket]] = {"t": [_packet("t", rows=1)]}
    buffer_rows: dict[str, int] = {"t": 1}
    called = {"legacy": 0, "chunked": 0}

    async def _flush_chunked_table(**kwargs: object) -> None:
        called["chunked"] += 1

    async def _flush_legacy_table(**kwargs: object) -> None:
        called["legacy"] += 1

    async def _handle_writer_flush_error(**kwargs: object) -> None:
        raise AssertionError("should not be called")

    await flush_writer_table(
        table="t",
        buffers=buffers,
        buffer_rows=buffer_rows,
        result=RunResult(provider="sharadar"),
        result_lock=asyncio.Lock(),
        get_table_schema=lambda _table: object(),
        writer_chunk_rows=None,
        flush_chunked_table=_flush_chunked_table,
        flush_legacy_table=_flush_legacy_table,
        handle_writer_flush_error=_handle_writer_flush_error,
        compute_error_cls=Exception,
        validation_error_cls=Exception,
        primary_key_missing_error_cls=PrimaryKeyMissingError,
        primary_key_null_error_cls=Exception,
        dlq_spool_error_cls=Exception,
        duckdb_module=None,
        logger=type("L", (), {"error": lambda *args, **kwargs: None, "exception": lambda *args, **kwargs: None})(),
    )
    assert called["legacy"] == 1
    assert called["chunked"] == 0
