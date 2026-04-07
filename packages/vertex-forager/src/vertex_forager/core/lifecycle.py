from __future__ import annotations

import asyncio
from contextlib import suppress
import time
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections import deque
    from pathlib import Path

from vertex_forager.core.checkpoint import Checkpoint, cleanup_state_retention
from vertex_forager.core.config import RunResult
from vertex_forager.utils import cleanup_dlq_tmp


def initialize_run_state(
    *,
    provider: str,
    dataset: str,
    checkpoint: Checkpoint | None,
    logger: Any,
) -> tuple[str, set[str], set[str]]:
    run_id = f"{provider}_{dataset}_{int(time.time())}"
    completed_symbols: set[str] = set()
    failed_symbols: set[str] = set()
    if checkpoint is None:
        return run_id, completed_symbols, failed_symbols
    run_id = checkpoint.run_id
    completed_symbols = set(checkpoint.completed)
    failed_symbols = set(checkpoint.failed)
    logger.info(
        "PIPELINE: Resuming from checkpoint %s, skipping %d completed symbols",
        run_id,
        len(completed_symbols),
    )
    return run_id, completed_symbols, failed_symbols


def create_run_queues(
    *,
    queue_max: int,
    checkpoint_retention_days: int,
    run_history_retention_days: int,
    dlq_tmp_retention_s: int,
    cache_dir: Path,
    logger: Any,
) -> tuple[
    asyncio.PriorityQueue[tuple[int, int, Any | None]],
    asyncio.Queue[Any | None],
]:
    req_q: asyncio.PriorityQueue[tuple[int, int, Any | None]] = asyncio.PriorityQueue(maxsize=queue_max)
    pkt_q: asyncio.Queue[Any | None] = asyncio.Queue(maxsize=queue_max)
    try:
        cleanup_state_retention(
            checkpoint_retention_days=checkpoint_retention_days,
            run_history_retention_days=run_history_retention_days,
            dlq_retention_s=dlq_tmp_retention_s,
        )
    except Exception as cleanup_error:
        logger.warning("PIPELINE: State retention cleanup failed: %s", cleanup_error)
    try:
        cleanup_dlq_tmp(cache_dir / "dlq", dlq_tmp_retention_s)
    except Exception as cleanup_error:
        logger.warning("PIPELINE: DLQ periodic cleanup failed: %s", cleanup_error)
    return req_q, pkt_q


def init_metrics_for_run() -> tuple[dict[str, int], dict[str, deque[float]], dict[str, float]]:
    counters: dict[str, int] = {"pipeline_runs": 1}
    return counters, {}, {}


def create_run_result(*, provider: str, run_id: str, dataset: str) -> tuple[RunResult, asyncio.Lock]:
    result = RunResult(provider=provider)
    result.run_id = run_id
    result.dataset = dataset
    result.started_at = time.time()
    return result, asyncio.Lock()


@runtime_checkable
class CounterSource(Protocol):
    def get_counters_and_reset(self) -> dict[str, int]: ...


class RunFinalizer:
    def __init__(
        self,
        *,
        mapper: object,
        writer: object,
        inc: Any,
        provider: str,
        sanitize_field: Any,
        logger: Any,
    ) -> None:
        self._mapper = mapper
        self._writer = writer
        self._inc = inc
        self._provider = provider
        self._sanitize_field = sanitize_field
        self._logger = logger

    def merge_component_counters(self) -> None:
        merge_component_counters(
            mapper=self._mapper,
            writer=self._writer,
            inc=self._inc,
        )

    def emit_pipeline_summary_log(
        self,
        *,
        dataset: str,
        started_monotonic: float,
        summary: dict[str, float],
    ) -> None:
        emit_pipeline_summary_log(
            provider=self._provider,
            dataset=dataset,
            started_monotonic=started_monotonic,
            summary=summary,
            sanitize_field=self._sanitize_field,
            logger=self._logger,
        )


def merge_component_counters(
    *,
    mapper: Any,
    writer: Any,
    inc: Any,
) -> None:
    with suppress(Exception):
        mapper_counters = getattr(mapper, "get_counters_and_reset", None)
        if callable(mapper_counters):
            for key, value in dict(mapper_counters()).items():
                inc(key, int(value))
    with suppress(Exception):
        writer_counters = getattr(writer, "get_counters_and_reset", None)
        if callable(writer_counters):
            for key, value in dict(writer_counters()).items():
                inc(key, int(value))


def emit_pipeline_summary_log(
    *,
    provider: str,
    dataset: str,
    started_monotonic: float,
    summary: dict[str, float],
    sanitize_field: Any,
    logger: Any,
) -> None:
    dur_run = time.monotonic() - started_monotonic
    extra: dict[str, object] = {
        "vf_provider": sanitize_field(provider),
        "vf_dataset": sanitize_field(dataset),
        "vf_symbol": "*",
        "vf_stage": "pipeline_summary",
        "vf_attempt": 0,
        "vf_duration_s": round(dur_run, 3),
    }
    extra.update({f"vf_{k}": round(v, 3) for k, v in sorted(summary.items())})
    logger.debug("vertex_forager stage", extra=extra)
