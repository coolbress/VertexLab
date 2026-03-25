from __future__ import annotations

import asyncio

import polars as pl
import pytest

from vertex_forager.core.config import RunResult
from vertex_forager.core.quality import parse_violation_count, validate_data_quality


def test_parse_violation_count_patterns() -> None:
    assert parse_violation_count("Column 'x' contains 7 negative values") == 7
    assert parse_violation_count("Found 3 duplicate rows") == 3
    assert parse_violation_count("some generic violation") == 1


@pytest.mark.asyncio
async def test_validate_data_quality_accumulates_violations(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Rule:
        def validate(self, df: pl.DataFrame) -> list[str]:
            assert df.height == 1
            return ["contains 2 bad values", "found 3 issues"]

    class _Schema:
        def __init__(self) -> None:
            self.quality_rules = [_Rule()]

    monkeypatch.setattr("vertex_forager.core.quality.get_table_schema", lambda _table: _Schema())
    result = RunResult(provider="sharadar")
    await validate_data_quality(
        table="sharadar_price",
        df=pl.DataFrame({"ticker": ["AAPL"]}),
        result=result,
        result_lock=asyncio.Lock(),
        logger=type("L", (), {"warning": lambda *args, **kwargs: None})(),
    )
    assert result.quality_violations["sharadar_price"] == 5
