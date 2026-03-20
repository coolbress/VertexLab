from __future__ import annotations

from typing import Any

from vertex_forager.utils import create_pbar_updater


class FakePbar:
    def __init__(self) -> None:
        self.updated = 0
        self.last_postfix = ""

    def update(self, n: int) -> None:
        self.updated += n

    def set_postfix_str(self, s: str, *, refresh: bool) -> None:
        self.last_postfix = s


class FakeParseResult:
    def __init__(self, next_jobs: list[Any] | None = None) -> None:
        self.next_jobs = next_jobs or []


class FakeJob:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol


def test_create_pbar_updater_accepts_arbitrary_signature() -> None:
    pbar = FakePbar()
    cb = create_pbar_updater(pbar)  # type: ignore[arg-type]

    # With pagination (next_jobs present) -> no update yet
    cb(job=FakeJob("AAPL"), parse_result=FakeParseResult(next_jobs=[1]), extra="ignored")
    assert pbar.updated == 0
    assert "Pagination" in pbar.last_postfix

    # Final page -> update count by number of tokens in symbol
    cb(job=FakeJob("AAPL,MSFT"), parse_result=FakeParseResult())
    assert pbar.updated == 2
