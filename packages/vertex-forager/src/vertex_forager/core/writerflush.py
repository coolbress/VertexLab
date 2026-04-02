from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import time
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import polars as pl

from vertex_forager.core.config import FramePacket, RunResult
from vertex_forager.core.errors import (
    PrimaryKeyMissingError,
    PrimaryKeyNullError,
    RunError,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from vertex_forager.writers.base import WriteResult

LogStructuredFunc = Callable[..., None]
IncFunc = Callable[[str, int], None]
ObserveFunc = Callable[[str, float], None]
AsyncFunc = Callable[..., Awaitable[None]]

_WRITER_CHUNK_ROWS = 20_000


@runtime_checkable
class LoggerLike(Protocol):
    def debug(self, msg: str, *args: object) -> None: ...
    def warning(self, msg: str, *args: object) -> None: ...
    def error(self, msg: str, *args: object) -> None: ...
    def exception(self, msg: str, *args: object) -> None: ...


@runtime_checkable
class WriterLike(Protocol):
    async def write(self, packet: FramePacket) -> WriteResult: ...


@runtime_checkable
class ConfigLike(Protocol):
    pass


@dataclass(frozen=True)
class WriterContext:
    writer: WriterLike
    config: ConfigLike
    logger: LoggerLike
    inc: IncFunc
    observe: ObserveFunc
    log_structured: LogStructuredFunc
    span: Callable[..., AbstractContextManager[object]]
    validate_data_quality: Callable[..., Awaitable[None]]
    get_table_schema: Callable[[str], object | None]
    spool_to_dlq_and_rescue: Callable[..., Awaitable[object]]
    build_writer_error_summary: Callable[..., RunError]
    compute_error_cls: type[Exception]
    validation_error_cls: type[Exception]
    dlq_spool_error_cls: type[Exception]
    duckdb_module: object | None
    progress_log_chunk_rows: int
    router_flexible_schema: bool


async def writer_worker(
    *,
    pkt_q: asyncio.Queue[FramePacket | None],
    result: RunResult,
    result_lock: asyncio.Lock,
    flush_threshold: int,
    flush_all_writer_buffers: AsyncFunc,
    buffer_or_flush_packet: AsyncFunc,
    flush_on_writer_cancel: AsyncFunc,
    dlq_spool_error_cls: type[Exception],
    logger: LoggerLike,
) -> None:
    buffers: dict[str, list[FramePacket]] = defaultdict(list)
    buffer_rows: dict[str, int] = defaultdict(int)
    threshold = flush_threshold
    logger.debug("WRITER: Adaptive bulk writing enabled. Threshold=%s rows", threshold)
    while True:
        got_item = False
        try:
            packet = await pkt_q.get()
            got_item = True
            if packet is None:
                logger.debug("WRITER: Received shutdown signal. Flushing remaining buffers...")
                await flush_all_writer_buffers(
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
                return
            await buffer_or_flush_packet(
                packet=packet,
                threshold=threshold,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
        except asyncio.CancelledError:
            await flush_on_writer_cancel(
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
            raise
        except Exception as exc:
            if not isinstance(exc, dlq_spool_error_cls):
                async with result_lock:
                    result.errors.append(RunError.from_exception(exc=exc, provider="", dataset="", symbol=""))
            logger.exception("WRITER: Unexpected error")
            raise
        finally:
            if got_item:
                pkt_q.task_done()


async def flush_on_writer_cancel(
    *,
    buffers: dict[str, list[FramePacket]],
    buffer_rows: dict[str, int],
    result: RunResult,
    result_lock: asyncio.Lock,
    flush_writer_table: AsyncFunc,
    logger: LoggerLike,
) -> None:
    for table in list(buffers.keys()):
        try:
            await asyncio.shield(
                flush_writer_table(
                    table=table,
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
            )
        except Exception as exc:
            logger.exception("WRITER: Error during cancellation flush for %s: %s", table, exc)


async def flush_all_writer_buffers(
    *,
    buffers: dict[str, list[FramePacket]],
    buffer_rows: dict[str, int],
    result: RunResult,
    result_lock: asyncio.Lock,
    flush_writer_table: AsyncFunc,
    logger: LoggerLike,
) -> None:
    first_exc: Exception | None = None
    for table in list(buffers.keys()):
        try:
            await flush_writer_table(
                table=table,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
        except Exception as exc:
            logger.exception("WRITER: Error during shutdown flush for %s: %s", table, exc)
            if first_exc is None:
                first_exc = exc
    if first_exc is not None:
        raise first_exc


async def buffer_or_flush_packet(
    *,
    packet: FramePacket,
    threshold: int,
    buffers: dict[str, list[FramePacket]],
    buffer_rows: dict[str, int],
    result: RunResult,
    result_lock: asyncio.Lock,
    flush_writer_table: AsyncFunc,
    progress_log_chunk_rows: int,
    logger: LoggerLike,
) -> None:
    table = packet.table
    buffers[table].append(packet)
    previous_rows = buffer_rows[table]
    current_rows = previous_rows + len(packet.frame)
    buffer_rows[table] = current_rows
    chunk_rows = max(1, int(progress_log_chunk_rows))
    if (current_rows // chunk_rows) > (previous_rows // chunk_rows):
        logger.debug("WRITER: Buffering %s... %d / %d rows", table, current_rows, threshold)
    if current_rows >= threshold:
        await flush_writer_table(
            table=table,
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
        )


def writer_table_context(packets: list[FramePacket]) -> tuple[str, str]:
    first_packet = packets[0] if packets else None
    provider = first_packet.provider if first_packet is not None else ""
    symbol = ""
    if first_packet is None:
        return provider, symbol
    context_symbol = first_packet.context.get("symbol")
    if isinstance(context_symbol, str):
        symbol = context_symbol
        return provider, symbol
    context_ticker = first_packet.context.get("ticker")
    if isinstance(context_ticker, str):
        symbol = context_ticker
    return provider, symbol


async def handle_writer_flush_error(
    *,
    table: str,
    packets: list[FramePacket],
    exc: Exception,
    prefix: str,
    buffers: dict[str, list[FramePacket]],
    buffer_rows: dict[str, int],
    result: RunResult,
    result_lock: asyncio.Lock,
    writer: WriterLike,
    config: ConfigLike,
    inc: IncFunc,
    log_structured: LogStructuredFunc,
    spool_to_dlq_and_rescue: Callable[..., Awaitable[object]],
    build_writer_error_summary: Callable[..., RunError],
    dlq_spool_error_cls: type[Exception],
    logger: LoggerLike,
) -> None:
    inc("errors_total", 1)
    provider, symbol = writer_table_context(packets)
    try:
        status = await spool_to_dlq_and_rescue(
            table=table,
            packets=packets,
            writer=writer,
            config=config,
            result=result,
            result_lock=result_lock,
            inc=inc,
            log_structured=log_structured,
        )
    except Exception as spool_exc:
        if isinstance(spool_exc, dlq_spool_error_cls):
            rescued_count = int(getattr(spool_exc, "rescued", 0))
            remaining_count = int(getattr(spool_exc, "remaining", 0))
            reported_spool_exc: Exception = spool_exc
        else:
            rescued_count = 0
            remaining_count = 0
            spool_error_factory: Callable[..., Exception] = dlq_spool_error_cls
            try:
                reported_spool_exc = spool_error_factory(
                    rescued=rescued_count,
                    remaining=remaining_count,
                    original=spool_exc,
                )
            except TypeError:
                reported_spool_exc = spool_error_factory(str(spool_exc))
        status = {
            "status": "spool_failed",
            "rescued": rescued_count,
            "remaining": remaining_count,
            "path": None,
            "error": reported_spool_exc,
        }
        summary = build_writer_error_summary(
            status=status,
            table=table,
            prefix=prefix,
            exc=exc,
            provider=provider,
            symbol=symbol,
        )
        async with result_lock:
            result.errors.append(summary)
        buffers[table] = []
        buffer_rows[table] = 0
        logger.exception("WRITER: Spool failed after %s for %s: %s", prefix, table, spool_exc)
        raise reported_spool_exc from spool_exc
    summary = build_writer_error_summary(
        status=status,
        table=table,
        prefix=prefix,
        exc=exc,
        provider=provider,
        symbol=symbol,
    )
    async with result_lock:
        result.errors.append(summary)
    buffers[table] = []
    buffer_rows[table] = 0


def concat_frames_with_flex(
    *,
    frames: list[pl.DataFrame],
    table_name: str,
    schema: object,
    rechunk: bool,
    router_flexible_schema: bool,
    logger: LoggerLike,
) -> pl.DataFrame:
    try:
        return pl.concat(frames, how="vertical", rechunk=rechunk)
    except pl.exceptions.PolarsError as exc:
        is_flexible = router_flexible_schema or (schema is not None and getattr(schema, "flexible_schema", False))
        if not is_flexible:
            raise
        logger.warning(
            "WRITER: Schema mismatch for %s: %s. Falling back to diagonal concat",
            table_name,
            exc,
        )
        return pl.concat(frames, how="diagonal")


def validate_unique_key(*, schema: object, table: str, frame: pl.DataFrame) -> None:
    unique_key = getattr(schema, "unique_key", None)
    if not unique_key:
        return
    for col in unique_key:
        if col not in frame.columns:
            raise PrimaryKeyMissingError(table=table, column=col)
        nulls = frame.get_column(col).null_count()
        if nulls > 0:
            raise PrimaryKeyNullError(table=table, column=col, null_count=nulls)


async def write_merged_packet(
    *,
    table: str,
    packet: FramePacket,
    stage: str,
    result: RunResult,
    result_lock: asyncio.Lock,
    writer: WriterLike,
    span: Callable[..., AbstractContextManager[object]],
    inc: IncFunc,
    observe: ObserveFunc,
    log_structured: LogStructuredFunc,
) -> None:
    t_w0 = time.monotonic()
    with span("write_flush", table=table, rows=len(packet.frame)):
        write_result = await writer.write(packet)
    t_w1 = time.monotonic()
    inc("writer_flushes", 1)
    observe("writer_flush_duration_s", float(t_w1 - t_w0))
    observe("writer_rows", float(write_result.rows))
    observe(f"writer_flush_duration_s.{table}", float(t_w1 - t_w0))
    observe(f"writer_rows.{table}", float(write_result.rows))
    inc("rows_written_total", int(write_result.rows))
    log_structured(
        provider=packet.provider,
        dataset=packet.table,
        symbol=None,
        stage=stage,
        duration_s=(t_w1 - t_w0),
    )
    async with result_lock:
        result.tables[write_result.table] = result.tables.get(write_result.table, 0) + write_result.rows


async def flush_chunked_table(
    *,
    table: str,
    packets: list[FramePacket],
    schema: object,
    chunk_size: int,
    buffers: dict[str, list[FramePacket]],
    buffer_rows: dict[str, int],
    result: RunResult,
    result_lock: asyncio.Lock,
    concat_frames_with_flex: Callable[..., pl.DataFrame],
    validate_unique_key: Callable[..., None],
    validate_data_quality: AsyncFunc,
    write_merged_packet: AsyncFunc,
    handle_writer_flush_error: AsyncFunc,
    compute_error_cls: type[Exception],
    validation_error_cls: type[Exception],
    primary_key_missing_error_cls: type[Exception],
    primary_key_null_error_cls: type[Exception],
    dlq_spool_error_cls: type[Exception],
    duckdb_module: object | None,
    log_structured: LogStructuredFunc,
    logger: LoggerLike,
) -> None:
    first = packets[0]
    total_rows_est = sum(len(p.frame) for p in packets)
    if total_rows_est > 0:
        est_chunks = (total_rows_est + chunk_size - 1) // chunk_size
        logger.debug(
            "WRITER: Chunking flush for %s rows~=%s chunk_size=%s chunks~=%s",
            table,
            total_rows_est,
            chunk_size,
            est_chunks,
        )
        log_structured(
            provider=first.provider,
            dataset=first.table,
            symbol=None,
            stage=f"write_chunking_rows_{total_rows_est}_size_{chunk_size}_chunks_{est_chunks}",
        )
    i = 0
    idx = 0
    while i < len(packets):
        rows_in_chunk = 0
        current_frames: list[pl.DataFrame] = []
        start_i = i
        while i < len(packets) and rows_in_chunk < chunk_size:
            pkt = packets[i]
            current_frames.append(pkt.frame)
            rows_in_chunk += len(pkt.frame)
            i += 1
        try:
            chunk_df = concat_frames_with_flex(
                frames=current_frames,
                table_name=first.table,
                schema=schema,
                rechunk=False,
            )
            await validate_data_quality(table=table, df=chunk_df, result=result, result_lock=result_lock)
            validate_unique_key(schema=schema, table=table, frame=chunk_df)
            chunk_packet = FramePacket(
                provider=first.provider,
                table=first.table,
                frame=chunk_df,
                observed_at=first.observed_at,
                context=first.context,
            )
            await write_merged_packet(
                table=table,
                packet=chunk_packet,
                stage=f"write_flush_chunk_{idx + 1}",
                result=result,
                result_lock=result_lock,
            )
            idx += 1
        except Exception as exc:
            if isinstance(exc, (compute_error_cls, validation_error_cls)):
                remaining_packets = packets[start_i : len(packets)]
                await handle_writer_flush_error(
                    table=table,
                    packets=remaining_packets,
                    exc=exc,
                    prefix="WriterError",
                    buffers=buffers,
                    buffer_rows=buffer_rows,
                    result=result,
                    result_lock=result_lock,
                )
                if isinstance(exc, primary_key_missing_error_cls):
                    logger.error("WRITER: PKMissing table=%s column=%s", table, getattr(exc, "column", ""))
                elif isinstance(exc, primary_key_null_error_cls):
                    logger.error(
                        "WRITER: PKNull table=%s column=%s nulls=%s",
                        table,
                        getattr(exc, "column", ""),
                        getattr(exc, "null_count", ""),
                    )
                else:
                    logger.error("WRITER: Error writing chunk for %s: %s", table, exc)
                return
            if isinstance(exc, dlq_spool_error_cls):
                raise
            remaining_packets = packets[start_i : len(packets)]
            is_duckdb_error = _is_duckdb_error(duckdb_module, exc)
            prefix = "DuckDBError" if is_duckdb_error else "UnexpectedWriterError"
            await handle_writer_flush_error(
                table=table,
                packets=remaining_packets,
                exc=exc,
                prefix=prefix,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
            if prefix == "DuckDBError":
                logger.exception("WRITER: DuckDB error for %s: %s", table, exc)
            else:
                logger.exception("WRITER: Unexpected error writing chunk for %s: %s", table, exc)
            return


async def flush_legacy_table(
    *,
    table: str,
    packets: list[FramePacket],
    schema: object,
    result: RunResult,
    result_lock: asyncio.Lock,
    concat_frames_with_flex: Callable[..., pl.DataFrame],
    validate_unique_key: Callable[..., None],
    validate_data_quality: AsyncFunc,
    write_merged_packet: AsyncFunc,
    logger: LoggerLike,
) -> None:
    first = packets[0]
    merged_frame = concat_frames_with_flex(
        frames=[p.frame for p in packets],
        table_name=first.table,
        schema=schema,
        rechunk=True,
    )
    validate_unique_key(schema=schema, table=table, frame=merged_frame)
    merged_packet = FramePacket(
        provider=first.provider,
        table=first.table,
        frame=merged_frame,
        observed_at=first.observed_at,
        context=first.context,
    )
    logger.debug("WRITER: Flushing %s packets (%s rows) for %s", len(packets), len(merged_frame), table)
    await validate_data_quality(table=table, df=merged_frame, result=result, result_lock=result_lock)
    await write_merged_packet(
        table=table,
        packet=merged_packet,
        stage="write_flush",
        result=result,
        result_lock=result_lock,
    )


async def flush_writer_table(
    *,
    table: str,
    buffers: dict[str, list[FramePacket]],
    buffer_rows: dict[str, int],
    result: RunResult,
    result_lock: asyncio.Lock,
    get_table_schema: Callable[[str], object | None],
    writer_chunk_rows: int | None,
    flush_chunked_table: AsyncFunc,
    flush_legacy_table: AsyncFunc,
    handle_writer_flush_error: AsyncFunc,
    compute_error_cls: type[Exception],
    validation_error_cls: type[Exception],
    primary_key_missing_error_cls: type[Exception],
    primary_key_null_error_cls: type[Exception],
    dlq_spool_error_cls: type[Exception],
    duckdb_module: object | None,
    logger: LoggerLike,
) -> None:
    packets = buffers.get(table, [])
    if not packets:
        return
    first = packets[0]
    schema = get_table_schema(first.table)
    chunk_size = writer_chunk_rows if writer_chunk_rows is not None else _WRITER_CHUNK_ROWS
    try:
        if isinstance(chunk_size, int) and chunk_size > 0:
            await flush_chunked_table(
                table=table,
                packets=packets,
                schema=schema,
                chunk_size=chunk_size,
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
        else:
            await flush_legacy_table(
                table=table,
                packets=packets,
                schema=schema,
                result=result,
                result_lock=result_lock,
            )
        buffers[table] = []
        buffer_rows[table] = 0
    except Exception as exc:
        if isinstance(exc, (compute_error_cls, validation_error_cls)):
            await handle_writer_flush_error(
                table=table,
                packets=packets,
                exc=exc,
                prefix="WriterError",
                buffers=buffers,
                buffer_rows=buffer_rows,
                result=result,
                result_lock=result_lock,
            )
            if isinstance(exc, primary_key_missing_error_cls):
                logger.error("WRITER: PKMissing table=%s column=%s", table, getattr(exc, "column", ""))
            elif isinstance(exc, primary_key_null_error_cls):
                logger.error(
                    "WRITER: PKNull table=%s column=%s nulls=%s",
                    table,
                    getattr(exc, "column", ""),
                    getattr(exc, "null_count", ""),
                )
            else:
                logger.error("WRITER: Error writing batch for %s: %s", table, exc)
            return
        if isinstance(exc, dlq_spool_error_cls):
            raise
        is_duckdb_error = _is_duckdb_error(duckdb_module, exc)
        prefix = "DuckDBError" if is_duckdb_error else "UnexpectedWriterError"
        await handle_writer_flush_error(
            table=table,
            packets=packets,
            exc=exc,
            prefix=prefix,
            buffers=buffers,
            buffer_rows=buffer_rows,
            result=result,
            result_lock=result_lock,
        )
        if prefix == "DuckDBError":
            logger.exception("WRITER: DuckDB error for %s: %s", table, exc)
        else:
            logger.exception("WRITER: Unexpected error writing batch for %s: %s", table, exc)


def _is_duckdb_error(duckdb_module: object | None, exc: Exception) -> bool:
    if duckdb_module is None:
        return False
    err_type = getattr(duckdb_module, "Error", None)
    return isinstance(err_type, type) and isinstance(exc, err_type)
