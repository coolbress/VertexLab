from __future__ import annotations

import asyncio

import polars as pl
import pytest

from vertex_forager.core.config import RunResult
from vertex_forager.core.quality import (
    NoDuplicateRows,
    NoFutureDates,
    NoNegativePrices,
    parse_violation_count,
    validate_data_quality,
)
from vertex_forager.exceptions import DataQualityError
from vertex_forager.providers.sharadar.schema import TABLES as SHARADAR_TABLES
from vertex_forager.providers.yfinance.schema import TABLES as YFINANCE_TABLES


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
        quality_check="warn",
        logger=type("L", (), {"warning": lambda *args, **kwargs: None})(),
    )
    assert result.quality_violations["sharadar_price"] == 5


@pytest.mark.asyncio
async def test_validate_data_quality_error_mode_raises_structured_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Rule:
        def validate(self, df: pl.DataFrame) -> list[str]:
            assert df.height == 1
            return ["Column 'close' contains 1 negative values"]

    class _Schema:
        def __init__(self) -> None:
            self.quality_rules = [_Rule()]

    monkeypatch.setattr("vertex_forager.core.quality.get_table_schema", lambda _table: _Schema())
    result = RunResult(provider="yfinance")

    with pytest.raises(DataQualityError) as exc_info:
        await validate_data_quality(
            table="yfinance_price",
            df=pl.DataFrame({"ticker": ["AAPL"], "close": [-1.0]}),
            result=result,
            result_lock=asyncio.Lock(),
            quality_check="error",
            logger=type("L", (), {"warning": lambda *args, **kwargs: None})(),
        )

    assert exc_info.value.table == "yfinance_price"
    assert exc_info.value.rule == "_Rule"
    assert exc_info.value.violations == ["Column 'close' contains 1 negative values"]
    assert result.quality_violations == {}


def test_run_result_serialization_hides_internal_metrics_fields() -> None:
    result = RunResult(
        provider="yfinance",
        data=pl.DataFrame({"ticker": ["AAPL"]}),
        metrics_counters={"rows_written_total": 1},
        metrics_histograms={"latency_s": [0.1]},
        dlq_pending={"yfinance_price": []},
    )

    dumped = result.model_dump(exclude={"data"})

    assert "metrics_counters" not in dumped
    assert "metrics_histograms" not in dumped
    assert "dlq_pending" not in dumped
    dumped_json = result.model_dump_json()
    assert "metrics_counters" not in dumped_json
    assert "metrics_histograms" not in dumped_json
    assert "dlq_pending" not in dumped_json


def test_sharadar_quality_rules_are_assigned() -> None:
    expected = {
        "sharadar_sep": (NoFutureDates, NoNegativePrices, NoDuplicateRows),
        "sharadar_tickers": (NoFutureDates, NoDuplicateRows),
        "sharadar_sf1": (NoFutureDates, NoDuplicateRows),
        "sharadar_sf2": (NoFutureDates, NoDuplicateRows),
        "sharadar_sf3": (NoFutureDates, NoDuplicateRows),
        "sharadar_actions": (NoFutureDates, NoDuplicateRows),
        "sharadar_daily": (NoFutureDates, NoDuplicateRows),
        "sharadar_sp500": (NoFutureDates, NoDuplicateRows),
    }

    for table, rule_types in expected.items():
        rules = SHARADAR_TABLES[table].quality_rules
        assert tuple(type(rule) for rule in rules) == rule_types


def test_yfinance_quality_rules_are_assigned() -> None:
    expected = {
        "yfinance_price": (NoFutureDates, NoNegativePrices, NoDuplicateRows),
        "yfinance_dividends": (NoFutureDates, NoDuplicateRows),
        "yfinance_splits": (NoFutureDates, NoDuplicateRows),
        "yfinance_actions": (NoFutureDates, NoDuplicateRows),
        "yfinance_financials": (NoFutureDates, NoDuplicateRows),
        "yfinance_holders": (NoFutureDates,),
        "yfinance_insider_roster_holders": (NoFutureDates,),
    }

    for table, rule_types in expected.items():
        rules = YFINANCE_TABLES[table].quality_rules
        assert tuple(type(rule) for rule in rules) == rule_types

    assert YFINANCE_TABLES["yfinance_calendar"].quality_rules == ()
    assert YFINANCE_TABLES["yfinance_info"].quality_rules == ()
    assert YFINANCE_TABLES["yfinance_fast_info"].quality_rules == ()
