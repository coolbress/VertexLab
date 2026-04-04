import logging
from unittest.mock import AsyncMock, MagicMock

from vertex_forager.core.config import ResolvedClientConfig
from vertex_forager.core.pipeline import VertexForager


def _make_engine(*, pickle_compat_datasets=()) -> VertexForager:
    cfg = ResolvedClientConfig(requests_per_minute=60)
    mock_router = MagicMock()
    mock_router.pickle_compat_datasets = tuple(pickle_compat_datasets)
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


def test_pickle_compat_datasets_emits_warning_and_metric(caplog) -> None:
    caplog.set_level(logging.WARNING, logger="vertex_forager.debug")
    engine = _make_engine(pickle_compat_datasets=("price", "financials"))
    msgs = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert any("pickle compatibility enabled for datasets=financials,price" in m for m in msgs)
    assert engine._counters.get("pickle_compat_enabled", 0) == 1


def test_no_warning_or_metric_without_pickle_compat_datasets(monkeypatch, caplog) -> None:
    monkeypatch.setenv("VF_ALLOW_PICKLE_COMPAT", "1")
    monkeypatch.setenv("VF_PICKLE_ALLOWED_DATASETS", "price")
    caplog.set_level(logging.WARNING, logger="vertex_forager.debug")
    engine = _make_engine()
    msgs = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert not any("pickle compatibility enabled" in m for m in msgs)
    assert engine._counters.get("pickle_compat_enabled", 0) == 0
