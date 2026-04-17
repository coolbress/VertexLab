from __future__ import annotations

import asyncio
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime
import os
import time

import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, FramePacket, RequestSpec, RetryConfig, RunResult
from vertex_forager.core.retry import RetryExecutor
from vertex_forager.core.writerflush import (
    FlushExecutor,
    FlushPlanner,
    FlushRecovery,
    MetricKind,
    flush_chunked_table,
)
from vertex_forager.exceptions import RunError

pytestmark = pytest.mark.manual


class _NoopLogger:
    @staticmethod
    def debug(msg: str, *args: object) -> None:
        pass

    @staticmethod
    def warning(msg: str, *args: object) -> None:
        pass

    @staticmethod
    def error(msg: str, *args: object) -> None:
        pass

    @staticmethod
    def exception(msg: str, *args: object) -> None:
        pass


class _NoopObserver:
    @staticmethod
    def on_metric(name: str, value: float, *, kind: MetricKind) -> None:
        pass

    @staticmethod
    def on_log(**kwargs: object) -> None:
        pass

    @contextmanager
    def span(self, name: str, **attributes: object) -> AbstractContextManager[object]:
        del self
        del name, attributes
        yield


class _NoopWriter:
    async def write(self, packet: FramePacket) -> object:
        return type("WriteResult", (), {"rows": len(packet.frame), "table": packet.table})()


class _Throttle:
    async def __aenter__(self) -> _Throttle:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _Controller:
    def throttle(self) -> _Throttle:
        return _Throttle()

    def record_feedback(self, *, status_code: int | None, retried: bool) -> None:
        pass


@pytest.mark.skipif(
    os.getenv("VF_ENABLE_CORE_PERF_BUDGET_TEST") != "1",
    reason="core perf budget test disabled by default",
)
@pytest.mark.asyncio
async def test_retry_and_writer_chunk_perf_budget() -> None:
    retry_budget_s = float(os.getenv("VF_PERF_BUDGET_RETRY_S", "1.5"))
    writer_budget_s = float(os.getenv("VF_PERF_BUDGET_WRITER_CHUNK_S", "1.0"))

    retry_executor = RetryExecutor(
        retry_config=RetryConfig(max_attempts=1),
        controller=_Controller(),
        http_fetch=lambda _spec: _return_bytes(),
        observe=lambda _name, _v: None,
        log_structured=lambda **kwargs: None,
    )
    job = FetchJob(
        provider="test",
        dataset="price",
        symbol="AAPL",
        spec=RequestSpec(url="https://example.test", params={}),
    )
    t0 = time.monotonic()
    for _ in range(200):
        payload = await retry_executor.fetch(job)
        assert payload == b"ok"
    retry_elapsed = time.monotonic() - t0
    assert retry_elapsed <= retry_budget_s

    packets = [
        _packet(rows=50, offset=i * 50)
        for i in range(10)
    ]
    t1 = time.monotonic()
    await flush_chunked_table(
        table="t",
        packets=packets,
        schema=object(),
        chunk_size=120,
        buffers={"t": packets.copy()},
        buffer_rows={"t": sum(len(p.frame) for p in packets)},
        result=RunResult(provider="test"),
        result_lock=asyncio.Lock(),
        planner=FlushPlanner(),
        executor=FlushExecutor(
            writer=_NoopWriter(),
            observer=_NoopObserver(),
            concat_frames_with_flex=lambda **kwargs: pl.concat(kwargs["frames"], how="vertical"),
            validate_unique_key=lambda **kwargs: None,
            validate_data_quality=lambda **kwargs: _noop_async(),
        ),
        recovery=FlushRecovery(
            writer=_NoopWriter(),
            config=object(),
            observer=_NoopObserver(),
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
            logger=_NoopLogger(),
        ),
        observer=_NoopObserver(),
        logger=_NoopLogger(),
    )
    writer_elapsed = time.monotonic() - t1
    assert writer_elapsed <= writer_budget_s


def _packet(*, rows: int, offset: int) -> FramePacket:
    return FramePacket(
        provider="test",
        table="t",
        frame=pl.DataFrame({"x": list(range(offset, offset + rows))}),
        observed_at=datetime.now(),
    )


async def _return_bytes() -> bytes:
    return b"ok"


async def _noop_async() -> None:
    return None


async def _noop_status() -> dict[str, object]:
    return {"status": "spooled", "rescued": 0, "remaining": 0, "path": None, "error": None}
