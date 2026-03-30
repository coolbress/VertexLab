from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import polars as pl
import pytest

from vertex_forager.core.config import FramePacket, RunResult
from vertex_forager.core.dlq import build_writer_error_summary, spool_to_dlq_and_rescue
from vertex_forager.writers.base import WriteResult


class _OkWriter:
    async def write(self, pkt: FramePacket) -> WriteResult:
        return WriteResult(table=pkt.table, rows=len(pkt.frame))


class _FailWriter:
    async def write(self, pkt: FramePacket) -> WriteResult:
        raise RuntimeError("boom")


def _packet(*, table: str, row_value: int) -> FramePacket:
    return FramePacket(
        provider="test",
        table=table,
        frame=pl.DataFrame({"id": [row_value]}),
        observed_at=datetime.now(timezone.utc),
    )


def _inc_collector() -> tuple[dict[str, int], Any]:
    counters: dict[str, int] = {}

    def _inc(name: str, value: int = 1) -> None:
        counters[name] = counters.get(name, 0) + int(value)

    return counters, _inc


@pytest.mark.asyncio
async def test_spool_to_dlq_and_rescue_noop() -> None:
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()
    counters, inc = _inc_collector()
    status = await spool_to_dlq_and_rescue(
        table="dlq_unit_unknown",
        packets=[],
        writer=_OkWriter(),
        config=SimpleNamespace(dlq_enabled=True, dlq_tmp_cleanup_on_error=False),
        result=result,
        result_lock=result_lock,
        inc=inc,
        log_structured=lambda **_: None,
    )
    assert status["status"] == "noop"
    assert counters == {}


@pytest.mark.asyncio
async def test_spool_to_dlq_and_rescue_disabled_counts() -> None:
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()
    counters, inc = _inc_collector()
    status = await spool_to_dlq_and_rescue(
        table="dlq_unit_unknown",
        packets=[_packet(table="dlq_unit_unknown", row_value=1), _packet(table="dlq_unit_unknown", row_value=2)],
        writer=_FailWriter(),
        config=SimpleNamespace(dlq_enabled=False, dlq_tmp_cleanup_on_error=False),
        result=result,
        result_lock=result_lock,
        inc=inc,
        log_structured=lambda **_: None,
    )
    assert status["status"] == "disabled"
    assert status["remaining"] >= 1
    assert result.dlq_counts["dlq_unit_unknown"]["remaining"] >= 1
    assert counters["dlq_disabled_total"] == 1


@pytest.mark.asyncio
async def test_spool_to_dlq_and_rescue_spools_to_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    import vertex_forager.core.checkpoint as ckpt_mod
    import vertex_forager.core.dlq as dlq_mod

    monkeypatch.setattr(dlq_mod, "get_cache_dir", lambda: tmp_path, raising=True)
    monkeypatch.setattr(ckpt_mod, "get_cache_dir", lambda: tmp_path, raising=True)
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()
    counters, inc = _inc_collector()
    log_events: list[str] = []

    def _log_structured(**kwargs: object) -> None:
        stage = kwargs.get("stage")
        if isinstance(stage, str):
            log_events.append(stage)

    status = await spool_to_dlq_and_rescue(
        table="dlq_unit_unknown",
        packets=[_packet(table="dlq_unit_unknown", row_value=1), _packet(table="dlq_unit_unknown", row_value=2)],
        writer=_FailWriter(),
        config=SimpleNamespace(dlq_enabled=True, dlq_tmp_cleanup_on_error=False),
        result=result,
        result_lock=result_lock,
        inc=inc,
        log_structured=_log_structured,
    )
    assert status["status"] == "spooled"
    assert status["path"] is not None
    assert status["remaining"] >= 1
    dlq_files = list((tmp_path / "dlq" / "dlq_unit_unknown").glob("batch_*.ipc"))
    assert dlq_files
    assert counters["dlq_spooled_files_total"] == 1
    assert any(stage.startswith("dlq_remaining_") for stage in log_events)


@pytest.mark.asyncio
async def test_spool_to_dlq_and_rescue_uses_unique_file_names_when_timestamps_match(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vertex_forager.core.checkpoint as ckpt_mod
    import vertex_forager.core.dlq as dlq_mod

    monkeypatch.setattr(dlq_mod, "get_cache_dir", lambda: tmp_path, raising=True)
    monkeypatch.setattr(ckpt_mod, "get_cache_dir", lambda: tmp_path, raising=True)
    monkeypatch.setattr(dlq_mod.time, "time_ns", lambda: 123456789, raising=True)
    uuids = iter(
        [
            UUID("00000000-0000-0000-0000-000000000001"),
            UUID("00000000-0000-0000-0000-000000000002"),
        ]
    )
    monkeypatch.setattr(dlq_mod, "uuid4", lambda: next(uuids), raising=True)
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()
    _counters, inc = _inc_collector()

    status_one = await spool_to_dlq_and_rescue(
        table="dlq_unit_unknown",
        packets=[_packet(table="dlq_unit_unknown", row_value=1)],
        writer=_FailWriter(),
        config=SimpleNamespace(dlq_enabled=True, dlq_tmp_cleanup_on_error=False),
        result=result,
        result_lock=result_lock,
        inc=inc,
        log_structured=lambda **_: None,
    )
    status_two = await spool_to_dlq_and_rescue(
        table="dlq_unit_unknown",
        packets=[_packet(table="dlq_unit_unknown", row_value=2)],
        writer=_FailWriter(),
        config=SimpleNamespace(dlq_enabled=True, dlq_tmp_cleanup_on_error=False),
        result=result,
        result_lock=result_lock,
        inc=inc,
        log_structured=lambda **_: None,
    )

    assert status_one["path"] != status_two["path"]


def test_build_writer_error_summary_spooled_context() -> None:
    status = {
        "status": "spooled",
        "rescued": 2,
        "remaining": 3,
        "path": "batch_1.ipc",
        "error": None,
    }
    err = build_writer_error_summary(
        status=status,
        table="prices",
        prefix="WriterError",
        exc=RuntimeError("disk full"),
        provider="test",
        symbol="AAPL",
    )
    assert "WriterError:prices:disk full" in err.message
    assert "DLQ=spooled" in err.message
    assert "rescued=2" in err.message
    assert "remaining=3" in err.message
    assert "path=batch_1.ipc" in err.message
