"""
Unit tests for pipeline internal components (Worker, etc).

현업 테스트 패턴:
- 파이프라인 내부 로직(Worker) 단위 테스트
- Mocking을 통한 격리
- 비동기 큐 처리 검증
"""

import asyncio
from datetime import datetime
import polars as pl
import pytest
from unittest.mock import AsyncMock, MagicMock

from vertex_forager.core.pipeline import VertexForager, RunResult
from vertex_forager.core.config import FramePacket, EngineConfig
from vertex_forager.writers.base import BaseWriter, WriteResult


@pytest.mark.asyncio
async def test_adaptive_batching_worker_drains_queue_correctly() -> None:
    """Verify that the worker drains the queue and calls writer.write with merged packets."""

    # 1. Setup Mocks
    mock_writer = AsyncMock(spec=BaseWriter)

    # Mock write to return a WriteResult, as required by the pipeline
    async def mock_write(pkt):
        return WriteResult(table=pkt.table, rows=len(pkt.frame))

    mock_writer.write.side_effect = mock_write

    # Mock other dependencies required for VertexForager initialization
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()

    forager = VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=EngineConfig(requests_per_minute=100),
        controller=mock_controller,
    )

    # 2. Setup Data
    pkt_q = asyncio.Queue()
    result = RunResult(provider="test")
    result_lock = asyncio.Lock()

    # Fill Queue with 10 small packets
    # Each packet has 1 row
    for i in range(10):
        df = pl.DataFrame({"id": [i]})
        packet = FramePacket(
            provider="test", table="batch_test", frame=df, observed_at=datetime.now()
        )
        pkt_q.put_nowait(packet)

    # Add None to stop the worker
    pkt_q.put_nowait(None)

    # 3. Run the worker
    # _writer_worker is an internal method, but testing it directly allows us
    # to verify batching logic without running the full pipeline.
    await forager._writer_worker(pkt_q=pkt_q, result=result, result_lock=result_lock)

    # 4. Assertions
    # With 10 packets in queue, the worker should have drained them all.
    assert mock_writer.write.called

    # Verify total rows processed
    # We can inspect the arguments passed to write to ensure no data was lost
    total_rows = 0
    for call in mock_writer.write.call_args_list:
        packet = call[0][0]  # First arg is packet
        total_rows += len(packet.frame)

    assert total_rows == 10

    # Verify RunResult was updated
    # Since we mocked write() to return correct rows, the worker should have updated the result
    assert "batch_test" in result.tables
    assert result.tables["batch_test"] == 10
