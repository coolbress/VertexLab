from __future__ import annotations

from pathlib import Path

import pytest

from vertex_forager.core.checkpoint import Checkpoint
from vertex_forager.core.lifecycle import (
    create_run_queues,
    create_run_result,
    emit_pipeline_summary_log,
    init_metrics_for_run,
    initialize_run_state,
    merge_component_counters,
)


def test_initialize_run_state_without_resume() -> None:
    run_id, completed, failed = initialize_run_state(
        provider="sharadar",
        dataset="price",
        resume=False,
        find_latest_checkpoint=lambda _p, _d: None,
        logger=type("L", (), {"info": lambda *args, **kwargs: None})(),
    )
    assert run_id.startswith("sharadar_price_")
    assert completed == set()
    assert failed == set()


def test_initialize_run_state_with_checkpoint() -> None:
    cp = Checkpoint(run_id="run-123", provider="sharadar", dataset="price", completed=["AAPL"], failed=["MSFT"])
    run_id, completed, failed = initialize_run_state(
        provider="sharadar",
        dataset="price",
        resume=True,
        find_latest_checkpoint=lambda _p, _d: cp,
        logger=type("L", (), {"info": lambda *args, **kwargs: None})(),
    )
    assert run_id == "run-123"
    assert completed == {"AAPL"}
    assert failed == {"MSFT"}


def test_create_run_queues_and_metrics_and_result() -> None:
    req_q, pkt_q = create_run_queues(
        queue_max=10,
        checkpoint_retention_days=7,
        run_history_retention_days=90,
        dlq_tmp_periodic_cleanup=False,
        dlq_tmp_retention_s=86400,
        cache_dir=Path("."),
        logger=type("L", (), {"warning": lambda *args, **kwargs: None})(),
    )
    assert req_q.maxsize == 10
    assert pkt_q.maxsize == 10

    counters, hists, summary = init_metrics_for_run(metrics_enabled=True)
    assert counters["pipeline_runs"] == 1
    assert hists == {}
    assert summary == {}

    result, lock = create_run_result(provider="sharadar", run_id="rid", dataset="price")
    assert result.provider == "sharadar"
    assert result.run_id == "rid"
    assert result.dataset == "price"
    assert lock is not None


def test_create_run_queues_passes_dlq_tmp_retention_s_to_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, int] = {}

    def _fake_cleanup(*, checkpoint_retention_days: int, run_history_retention_days: int, dlq_retention_s: int) -> None:
        captured["dlq_retention_s"] = dlq_retention_s

    monkeypatch.setattr("vertex_forager.core.lifecycle.cleanup_state_retention", _fake_cleanup)

    custom_retention = 3600
    create_run_queues(
        queue_max=10,
        checkpoint_retention_days=7,
        run_history_retention_days=90,
        dlq_tmp_periodic_cleanup=False,
        dlq_tmp_retention_s=custom_retention,
        cache_dir=Path("."),
        logger=type("L", (), {"warning": lambda *args, **kwargs: None})(),
    )
    assert captured.get("dlq_retention_s") == custom_retention


def test_merge_component_counters_merges_from_mapper_and_writer() -> None:
    counters: dict[str, int] = {}

    class _Mapper:
        @staticmethod
        def get_counters_and_reset() -> dict[str, int]:
            return {"mapper_rows": 2}

    class _Writer:
        @staticmethod
        def get_counters_and_reset() -> dict[str, int]:
            return {"writer_rows": 3}

    def _inc(name: str, amount: int) -> None:
        counters[name] = counters.get(name, 0) + amount

    merge_component_counters(
        mapper=_Mapper(),
        writer=_Writer(),
        inc=_inc,
    )
    assert counters["mapper_rows"] == 2
    assert counters["writer_rows"] == 3


def test_emit_pipeline_summary_log_writes_debug_line() -> None:
    messages: list[str] = []

    class _Logger:
        @staticmethod
        def debug(message: str) -> None:
            messages.append(message)

        @staticmethod
        def info(message: str) -> None:
            messages.append(message)

    emit_pipeline_summary_log(
        structured_logs=True,
        log_verbose=False,
        provider="sharadar",
        dataset="price",
        started_monotonic=0.0,
        summary={"http_duration_s_p95": 1.2},
        sanitize_field=lambda x: x,
        logger=_Logger(),
    )
    assert any("stage=pipeline_summary" in message for message in messages)
