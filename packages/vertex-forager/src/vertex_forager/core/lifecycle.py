from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
import time
from typing import Any, Protocol, runtime_checkable

from vertex_forager.core.checkpoint import Checkpoint
from vertex_forager.core.config import RunResult
from vertex_forager.utils import cleanup_dlq_tmp


def initialize_run_state(
    *,
    provider: str,
    dataset: str,
    resume: bool,
    find_latest_checkpoint: Callable[[str, str], Checkpoint | None],
    logger: Any,
) -> tuple[str, set[str], set[str]]:
    run_id = f"{provider}_{dataset}_{int(time.time())}"
    completed_symbols: set[str] = set()
    failed_symbols: set[str] = set()
    if not resume:
        return run_id, completed_symbols, failed_symbols
    latest_checkpoint = find_latest_checkpoint(provider, dataset)
    if latest_checkpoint is None:
        logger.info(
            "PIPELINE: No checkpoint found for provider %s and dataset %s, starting fresh",
            provider,
            dataset,
        )
        return run_id, completed_symbols, failed_symbols
    run_id = latest_checkpoint.run_id
    completed_symbols = set(latest_checkpoint.completed)
    failed_symbols = set(latest_checkpoint.failed)
    logger.info(
        "PIPELINE: Resuming from checkpoint %s, skipping %d completed symbols",
        run_id,
        len(completed_symbols),
    )
    return run_id, completed_symbols, failed_symbols


def create_run_queues(
    *,
    queue_max: int,
    dlq_tmp_periodic_cleanup: bool,
    dlq_tmp_retention_s: int,
    cache_dir: Path,
    logger: Any,
) -> tuple[
    asyncio.PriorityQueue[tuple[int, int, Any | None]],
    asyncio.Queue[Any | None],
]:
    req_q: asyncio.PriorityQueue[tuple[int, int, Any | None]] = asyncio.PriorityQueue(maxsize=queue_max)
    pkt_q: asyncio.Queue[Any | None] = asyncio.Queue(maxsize=queue_max)
    if dlq_tmp_periodic_cleanup:
        try:
            cleanup_dlq_tmp(cache_dir / "dlq", dlq_tmp_retention_s)
        except Exception as cleanup_error:
            logger.warning("PIPELINE: DLQ periodic cleanup failed: %s", cleanup_error)
    return req_q, pkt_q


def init_metrics_for_run(*, metrics_enabled: bool) -> tuple[dict[str, int], dict[str, deque[float]], dict[str, float]]:
    if not metrics_enabled:
        return {}, {}, {}
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
        structured_logs: bool,
        log_verbose: bool,
        provider: str,
        sanitize_field: Any,
        logger: Any,
    ) -> None:
        self._mapper = mapper
        self._writer = writer
        self._inc = inc
        self._structured_logs = structured_logs
        self._log_verbose = log_verbose
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
            structured_logs=self._structured_logs,
            log_verbose=self._log_verbose,
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
    structured_logs: bool,
    log_verbose: bool,
    provider: str,
    dataset: str,
    started_monotonic: float,
    summary: dict[str, float],
    sanitize_field: Any,
    logger: Any,
) -> None:
    if not structured_logs:
        return
    dur_run = time.monotonic() - started_monotonic
    msg_s = (
        f"OBS provider={sanitize_field(provider)} "
        f"dataset={sanitize_field(dataset)} symbol=* stage=pipeline_summary attempt=0 "
        f"duration={dur_run:.3f}s " + " ".join(f"{k}={v:.3f}" for k, v in sorted(summary.items()))
    )
    if log_verbose:
        logger.info(msg_s)
    else:
        logger.debug(msg_s)
