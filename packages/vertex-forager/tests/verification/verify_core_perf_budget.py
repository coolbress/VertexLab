from __future__ import annotations

import asyncio
from datetime import datetime
import os
import time

import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, FramePacket, RequestSpec, RetryConfig, RunResult
from vertex_forager.core.retry import RetryExecutor
from vertex_forager.core.writerflush import flush_chunked_table

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
        concat_frames_with_flex=lambda **kwargs: pl.concat(kwargs["frames"], how="vertical"),
        validate_unique_key=lambda **kwargs: None,
        validate_data_quality=lambda **kwargs: _noop_async(),
        write_merged_packet=lambda **kwargs: _noop_async(),
        handle_writer_flush_error=lambda **kwargs: _noop_async(),
        compute_error_cls=Exception,
        validation_error_cls=Exception,
        primary_key_missing_error_cls=Exception,
        primary_key_null_error_cls=Exception,
        dlq_spool_error_cls=Exception,
        duckdb_module=None,
        log_structured=lambda **kwargs: None,
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
