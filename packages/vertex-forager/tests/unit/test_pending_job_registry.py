from __future__ import annotations

from vertex_forager.core.config import FetchJob, RequestSpec
from vertex_forager.core.runtime_state import PendingJobRegistry


def _job(*, symbol: str | None, page: int | None = None) -> FetchJob:
    params = {} if page is None else {"page": page}
    return FetchJob(
        provider="stub",
        dataset="price",
        symbol=symbol,
        spec=RequestSpec(url="https://example.test", params=params),
    )


def test_pending_job_registry_dedups_and_preserves_order() -> None:
    first = _job(symbol="AAPL")
    duplicate = _job(symbol="AAPL")
    second = _job(symbol="MSFT", page=2)
    extra = _job(symbol=None, page=3)

    registry = PendingJobRegistry([first])
    registry.add([duplicate, second])

    assert len(registry) == 2
    assert registry.snapshot([second, extra]) == [first, second, extra]


def test_pending_job_registry_remove_and_clear() -> None:
    first = _job(symbol="AAPL")
    second = _job(symbol="MSFT")
    registry = PendingJobRegistry([first, second])

    registry.remove(_job(symbol="AAPL"))
    assert first not in registry
    assert registry.snapshot() == [second]

    registry.clear()
    assert len(registry) == 0
    assert registry.snapshot() == []
