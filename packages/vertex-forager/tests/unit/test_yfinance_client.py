"""Unit tests for YFinance client and router behavior."""

from __future__ import annotations

from typing import Any, cast

import pytest

from vertex_forager.clients import create_client
from vertex_forager.exceptions import InputError

try:
    from vertex_forager.providers.yfinance.client import YFinanceClient
    from vertex_forager.providers.yfinance.router import YFinanceRouter
except ImportError:
    YFinanceClient = None
    YFinanceRouter = None

pytestmark = pytest.mark.skipif(
    YFinanceClient is None or YFinanceRouter is None,
    reason="requires optional dependency: vertex-forager[yfinance]",
)


class TestYFinanceClientDefaults:
    """Verify default configuration and client creation behavior."""

    def test_client_init_defaults(self) -> None:
        client = YFinanceClient()
        assert client.api_key is None
        assert client._config.requests_per_minute == 60
        assert client._pickle_compat_datasets is None

    def test_client_init_accepts_pickle_compat_datasets(self) -> None:
        client = YFinanceClient(pickle_compat_datasets=["price", " price ", "financials"])

        assert client._pickle_compat_datasets == ["price", "financials"]

    def test_client_init_rejects_non_list_pickle_compat_datasets(self) -> None:
        with pytest.raises(InputError, match="pickle_compat_datasets must be a list"):
            YFinanceClient(pickle_compat_datasets="price")  # type: ignore[arg-type]

    def test_create_client_without_api_key(self) -> None:
        client = create_client(provider="yfinance")
        assert isinstance(client, YFinanceClient)
        assert client.api_key is None
        assert client._config.requests_per_minute == 60

    def test_create_client_accepts_pickle_compat_datasets_for_yfinance(self) -> None:
        client = create_client(
            provider="yfinance",
            pickle_compat_datasets=["price"],
        )

        assert isinstance(client, YFinanceClient)
        assert client._pickle_compat_datasets == ["price"]

    def test_create_client_ignores_user_api_key(self) -> None:
        with pytest.raises(TypeError):
            create_client(
                provider="yfinance",
                api_key="user_supplied",
            )


class TestYFinanceRouterDateParams:
    """Verify date-parameter behavior for YFinanceRouter price jobs."""

    @pytest.mark.asyncio
    async def test_price_jobs_without_dates_omit_start_end(self) -> None:
        router = YFinanceRouter(rate_limit=60, start_date=None, end_date=None)
        jobs = [job async for job in router.generate_jobs(dataset="price", symbols=["AAPL", "MSFT"])]
        assert len(jobs) == 2
        for job in jobs:
            params = job.spec.params
            params_dict = cast("dict[str, Any]", params)
            lib = cast("dict[str, Any]", params_dict.get("lib", {}))
            lib_kwargs = cast("dict[str, Any]", lib.get("kwargs", {}))
            assert "start" not in lib_kwargs
            assert "end" not in lib_kwargs

    @pytest.mark.asyncio
    async def test_generate_jobs_invalid_symbols_raises(self) -> None:
        router = YFinanceRouter(rate_limit=60, start_date=None, end_date=None)
        with pytest.raises(ValueError, match=r".*"):
            _ = [job async for job in router.generate_jobs(dataset="price", symbols=["", "   ", "@@"])]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "kwargs", "expected_dataset", "expected_table"),
    [
        ("get_info", {"tickers": ["AAPL"]}, "info", "yfinance_info"),
        ("get_fast_info", {"tickers": ["AAPL"]}, "fast_info", "yfinance_fast_info"),
        ("get_price_data", {"tickers": ["AAPL"]}, "price", "yfinance_price"),
        ("get_actions", {"kind": "dividends", "tickers": ["AAPL"]}, "dividends", "yfinance_dividends"),
        ("get_actions", {"kind": "actions", "tickers": ["AAPL"]}, "actions", "yfinance_actions"),
        (
            "get_holders",
            {"kind": "institutional", "tickers": ["AAPL"]},
            "institutional_holders",
            "yfinance_holders",
        ),
        ("get_major_holders", {"tickers": ["AAPL"]}, "major_holders", "yfinance_major_holders"),
        (
            "get_insider_roster_holders",
            {"tickers": ["AAPL"]},
            "insider_roster_holders",
            "yfinance_insider_roster_holders",
        ),
        ("get_insider_purchases", {"tickers": ["AAPL"]}, "insider_purchases", "yfinance_insider_purchases"),
        ("get_calendar", {"tickers": ["AAPL"]}, "calendar", "yfinance_calendar"),
        ("get_recommendations", {"tickers": ["AAPL"]}, "recommendations", "yfinance_recommendations"),
        ("get_news", {"tickers": ["AAPL"]}, "news", "yfinance_news"),
    ],
)
async def test_public_methods_dispatch_to_expected_dataset_async(
    method_name: str,
    kwargs: dict[str, Any],
    expected_dataset: str,
    expected_table: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = YFinanceClient()
    captured: dict[str, Any] = {}

    async def _fake_dispatch_fetch(**dispatch_kwargs: Any) -> dict[str, Any]:
        captured.update(dispatch_kwargs)
        return {"ok": True}

    monkeypatch.setattr(client, "_dispatch_fetch", _fake_dispatch_fetch)
    raw_method = getattr(client, f"{method_name}_async")
    result = await raw_method(connect_db=None, progress=False, **kwargs)
    assert result == {"ok": True}
    assert captured["dataset"] == expected_dataset
    assert captured["table_name"] == expected_table
    assert captured["tickers"] == ["AAPL"]


def test_get_info_sync_facade_dispatches(monkeypatch: pytest.MonkeyPatch) -> None:
    client = YFinanceClient()
    captured: dict[str, Any] = {}

    async def _fake_dispatch_fetch(**dispatch_kwargs: Any) -> dict[str, Any]:
        captured.update(dispatch_kwargs)
        return {"ok": True}

    monkeypatch.setattr(client, "_dispatch_fetch", _fake_dispatch_fetch)
    result = client.get_info(tickers=["AAPL"], connect_db=None, progress=False)
    assert result == {"ok": True}
    assert captured["dataset"] == "info"
    assert captured["table_name"] == "yfinance_info"
    assert captured["tickers"] == ["AAPL"]


@pytest.mark.asyncio
async def test_get_financials_maps_earnings_to_financials_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = YFinanceClient()
    captured: dict[str, Any] = {}

    async def _fake_dispatch_fetch(**dispatch_kwargs: Any) -> dict[str, Any]:
        captured.update(dispatch_kwargs)
        return {"ok": True}

    monkeypatch.setattr(client, "_dispatch_fetch", _fake_dispatch_fetch)
    result = await client.get_financials_async(
        kind="earnings",
        period="annual",
        tickers=["AAPL"],
        connect_db=None,
        progress=False,
    )
    assert result == {"ok": True}
    assert captured["dataset"] == "financials"
    assert captured["table_name"] == "yfinance_financials"


@pytest.mark.asyncio
async def test_get_financials_quarterly_income_stmt_dataset_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = YFinanceClient()
    captured: dict[str, Any] = {}

    async def _fake_dispatch_fetch(**dispatch_kwargs: Any) -> dict[str, Any]:
        captured.update(dispatch_kwargs)
        return {"ok": True}

    monkeypatch.setattr(client, "_dispatch_fetch", _fake_dispatch_fetch)
    result = await client.get_financials_async(
        kind="income_stmt",
        period="quarterly",
        tickers=["AAPL"],
        connect_db=None,
        progress=False,
    )
    assert result == {"ok": True}
    assert captured["dataset"] == "quarterly_financials"


@pytest.mark.asyncio
async def test_get_financials_rejects_quarterly_earnings_async() -> None:
    client = YFinanceClient()
    with pytest.raises(InputError, match="quarterly_earnings"):
        await client.get_financials_async(
            kind="earnings",
            period="quarterly",
            tickers=["AAPL"],
            connect_db=None,
            progress=False,
        )


@pytest.mark.asyncio
async def test_dispatch_fetch_filters_pipeline_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    client = YFinanceClient(pickle_compat_datasets=["price"])
    captured: dict[str, Any] = {}

    class _Ctx:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    monkeypatch.setattr(client, "managed_writer", lambda connect_db, show_progress=True: _Ctx())
    monkeypatch.setattr(
        "vertex_forager.providers.yfinance.client.validate_tickers",
        lambda symbols: None,
    )
    monkeypatch.setattr(
        "vertex_forager.providers.yfinance.client.validate_memory_usage",
        lambda **kwargs: None,
    )

    async def _fake_run_pipeline(**kwargs: Any) -> None:
        captured["run_pipeline_kwargs"] = kwargs

    async def _fake_collect_results(**kwargs: Any) -> dict[str, Any]:
        captured["collect_results_kwargs"] = kwargs
        return {"done": True}

    def _fake_create_router(provider: str, **kwargs: Any) -> object:
        captured["router_provider"] = provider
        captured["router_kwargs"] = kwargs
        return object()

    monkeypatch.setattr(client, "run_pipeline", _fake_run_pipeline)
    monkeypatch.setattr(client, "collect_results", _fake_collect_results)
    monkeypatch.setattr("vertex_forager.providers.yfinance.client.create_router", _fake_create_router)

    result = await client._dispatch_fetch(
        dataset="price",
        tickers=["AAPL"],
        connect_db=None,
        table_name="yfinance_price",
        progress=True,
        price_batch_size=123,
        custom_flag=True,
    )

    assert result == {"done": True}
    assert captured["router_provider"] == "yfinance"
    assert captured["router_kwargs"]["rate_limit"] == client.config.requests_per_minute
    assert captured["router_kwargs"]["price_batch_size"] == 123
    assert captured["router_kwargs"]["pickle_compat_datasets"] == ["price"]
    assert "custom_flag" not in captured["router_kwargs"]
    assert captured["run_pipeline_kwargs"]["dataset"] == "price"
    assert captured["run_pipeline_kwargs"]["symbols"] == ["AAPL"]
    assert captured["run_pipeline_kwargs"]["custom_flag"] is True
    assert captured["run_pipeline_kwargs"]["progress"] is True
    assert "price_batch_size" not in captured["run_pipeline_kwargs"]
