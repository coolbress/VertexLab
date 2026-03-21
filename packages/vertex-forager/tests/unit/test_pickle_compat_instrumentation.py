import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from vertex_forager.core.config import EngineConfig
from vertex_forager.core.pipeline import VertexForager


def _make_engine(metrics_enabled: bool) -> VertexForager:
    cfg = EngineConfig(requests_per_minute=60, metrics_enabled=metrics_enabled)
    mock_router = MagicMock()
    mock_http = MagicMock()
    mock_writer = AsyncMock()
    mock_mapper = MagicMock()
    mock_controller = MagicMock()
    mock_controller.concurrency_limit = 1
    return VertexForager(
        router=mock_router,
        http=mock_http,
        writer=mock_writer,
        mapper=mock_mapper,
        config=cfg,
        controller=mock_controller,
    )


def test_pickle_compat_env_emits_warning_and_metric(monkeypatch, caplog) -> None:
    monkeypatch.setenv("VF_ALLOW_PICKLE_COMPAT", "1")
    caplog.set_level(logging.WARNING, logger="vertex_forager.debug")
    engine = _make_engine(metrics_enabled=True)
    msgs = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert any("VF_ALLOW_PICKLE_COMPAT is enabled" in m for m in msgs)
    assert engine._counters.get("pickle_compat_enabled", 0) == 1


def test_no_warning_or_metric_without_env(monkeypatch, caplog) -> None:
    with pytest.raises(KeyError):
        monkeypatch.delenv("VF_ALLOW_PICKLE_COMPAT")
    caplog.set_level(logging.WARNING, logger="vertex_forager.debug")
    engine = _make_engine(metrics_enabled=True)
    msgs = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert not any("VF_ALLOW_PICKLE_COMPAT is enabled" in m for m in msgs)
    assert engine._counters.get("pickle_compat_enabled", 0) == 0
