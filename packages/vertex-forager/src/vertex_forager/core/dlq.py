from __future__ import annotations

from contextlib import suppress
import os
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl

from vertex_forager.core.checkpoint import get_cache_dir, register_dlq_entry
from vertex_forager.core.errors import DLQSpoolError, RunError
from vertex_forager.schema.registry import get_table_schema

if TYPE_CHECKING:
    import asyncio

    from vertex_forager.core.config import FramePacket, RunResult
    from vertex_forager.core.types import DLQStatus


async def _update_dlq_counts(
    *,
    table: str,
    rescued: int,
    remaining: int,
    result: RunResult,
    result_lock: asyncio.Lock,
) -> None:
    async with result_lock:
        entry = result.dlq_counts.get(table) or {"rescued": 0, "remaining": 0}
        if rescued:
            entry["rescued"] = entry.get("rescued", 0) + int(rescued)
        if remaining:
            entry["remaining"] = entry.get("remaining", 0) + int(remaining)
        result.dlq_counts[table] = entry


def _pk_invalid_reason(pkt: FramePacket) -> str | None:
    schema = get_table_schema(pkt.table)
    if not schema or not schema.unique_key:
        return None
    for col in schema.unique_key:
        if col not in pkt.frame.columns:
            return "missing"
        if pkt.frame.get_column(col).null_count() > 0:
            return "null"
    return None


async def _rescue_packets(
    *,
    packets: list[FramePacket],
    writer: Any,
    result: RunResult,
    result_lock: asyncio.Lock,
) -> tuple[int, list[FramePacket]]:
    rescued = 0
    failed_packets: list[FramePacket] = []
    consecutive_failures = 0
    max_consecutive_failures = 3
    for pkt in packets:
        reason = _pk_invalid_reason(pkt)
        if reason is not None:
            failed_packets.append(pkt)
            consecutive_failures += 1
        else:
            try:
                wr = await writer.write(pkt)
                async with result_lock:
                    result.tables[wr.table] = result.tables.get(wr.table, 0) + wr.rows
                rescued += 1
                consecutive_failures = 0
            except Exception:
                failed_packets.append(pkt)
                consecutive_failures += 1
        if consecutive_failures >= max_consecutive_failures:
            processed = rescued + len(failed_packets)
            if processed < len(packets):
                failed_packets.extend(packets[processed:])
            break
    return rescued, failed_packets


def _spool_failed_packets(
    *,
    table: str,
    failed_packets: list[FramePacket],
) -> str:
    frames = [p.frame for p in failed_packets]
    try:
        merged = pl.concat(frames, how="vertical", rechunk=True)
    except pl.exceptions.PolarsError:
        merged = pl.concat(frames, how="diagonal")
    dlq_dir = get_cache_dir() / "dlq" / table
    dlq_dir.mkdir(parents=True, exist_ok=True)
    ts_ns = time.time_ns()
    fpath = dlq_dir / f"batch_{ts_ns}_{uuid4().hex}.ipc"
    tmp_path = fpath.parent / (f"{fpath.name}.tmp")
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        merged.write_ipc(fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, fpath)
    with suppress(Exception):
        dir_fd = os.open(str(dlq_dir), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    try:
        register_dlq_entry(
            path=fpath,
            table=table,
            provider=failed_packets[0].provider if failed_packets else None,
            row_count=int(merged.height),
            output_uri=None,
        )
    except Exception:
        fpath.unlink(missing_ok=True)
        raise
    return str(fpath)


async def _on_spool_failed(
    *,
    table: str,
    failed_packets: list[FramePacket],
    rescued: int,
    exc: Exception,
    inc: Any,
    result: RunResult,
    result_lock: asyncio.Lock,
) -> None:
    async with result_lock:
        pending = result.dlq_pending.get(table, [])
        pending.extend(failed_packets)
        result.dlq_pending[table] = pending
    remaining_count = len(failed_packets)
    inc("dlq_spool_failed_total", 1)
    if rescued:
        inc("dlq_rescued_total", int(rescued))
        inc(f"dlq_rescued.{table}", int(rescued))
    if remaining_count:
        inc("dlq_remaining_total", int(remaining_count))
        inc(f"dlq_remaining.{table}", int(remaining_count))
    await _update_dlq_counts(
        table=table,
        rescued=rescued,
        remaining=remaining_count,
        result=result,
        result_lock=result_lock,
    )
    raise DLQSpoolError(rescued=rescued, remaining=remaining_count, original=exc) from exc


async def spool_to_dlq_and_rescue(
    *,
    table: str,
    packets: list[FramePacket],
    writer: Any,
    config: Any,
    result: RunResult,
    result_lock: asyncio.Lock,
    inc: Any,
    log_structured: Any,
) -> DLQStatus:
    if not packets:
        return {"status": "noop", "rescued": 0, "remaining": 0, "path": None, "error": None}
    rescued, failed_packets = await _rescue_packets(
        packets=packets,
        writer=writer,
        result=result,
        result_lock=result_lock,
    )
    remaining = len(failed_packets)
    if remaining == 0:
        if rescued:
            inc("dlq_rescued_total", int(rescued))
            inc(f"dlq_rescued.{table}", int(rescued))
        await _update_dlq_counts(
            table=table,
            rescued=rescued,
            remaining=0,
            result=result,
            result_lock=result_lock,
        )
        return {"status": "rescued_only", "rescued": rescued, "remaining": 0, "path": None, "error": None}
    try:
        path = _spool_failed_packets(table=table, failed_packets=failed_packets)
    except Exception as exc:
        await _on_spool_failed(
            table=table,
            failed_packets=failed_packets,
            rescued=rescued,
            exc=exc,
            inc=inc,
            result=result,
            result_lock=result_lock,
        )
    if rescued:
        inc("dlq_rescued_total", int(rescued))
        inc(f"dlq_rescued.{table}", int(rescued))
    inc("dlq_spooled_files_total", 1)
    inc(f"dlq_spooled_files.{table}", 1)
    inc("dlq_remaining_total", int(remaining))
    inc(f"dlq_remaining.{table}", int(remaining))
    await _update_dlq_counts(
        table=table,
        rescued=rescued,
        remaining=remaining,
        result=result,
        result_lock=result_lock,
    )
    log_structured(provider=packets[0].provider, dataset=table, symbol=None, stage=f"dlq_rescued_{rescued}")
    log_structured(provider=packets[0].provider, dataset=table, symbol=None, stage=f"dlq_remaining_{remaining}")
    return {"status": "spooled", "rescued": rescued, "remaining": remaining, "path": path, "error": None}


def build_writer_error_summary(
    *,
    status: DLQStatus,
    table: str,
    prefix: str,
    exc: Exception,
    provider: str,
    symbol: str,
) -> RunError:
    dlq_context_parts = []
    match status["status"]:
        case "spooled":
            dlq_context_parts.append("DLQ=spooled")
            if status.get("remaining"):
                dlq_context_parts.append(f"remaining={status['remaining']}")
            if status.get("rescued"):
                dlq_context_parts.append(f"rescued={status['rescued']}")
            if status.get("path"):
                dlq_context_parts.append(f"path={os.path.basename(str(status['path']))}")
        case "rescued_only":
            dlq_context_parts.append("DLQ=rescued_only")
            if status.get("rescued"):
                dlq_context_parts.append(f"rescued={status['rescued']}")
        case "spool_failed":
            dlq_context_parts.append("DLQ=spool_failed")
        case "disabled":
            dlq_context_parts.append("DLQ=disabled")
        case "noop":
            dlq_context_parts.append("DLQ=noop")
        case _:
            dlq_context_parts.append(f"DLQ={status['status']}")
    dlq_context = " ".join(dlq_context_parts)
    message = f"{prefix}:{table}:{exc!s}"
    if dlq_context:
        message = f"{message} {dlq_context}"
    original_error = RunError.from_exception(
        exc=exc,
        provider=provider,
        dataset=table,
        symbol=symbol,
    )
    return RunError(
        provider=original_error.provider,
        dataset=original_error.dataset,
        symbol=original_error.symbol,
        exc_type=original_error.exc_type,
        message=message,
        retryable=original_error.retryable,
    )
