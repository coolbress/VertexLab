from __future__ import annotations

import importlib.util

import vertex_forager
from vertex_forager import api as public_api
from vertex_forager.providers.sharadar.client import SharadarClient


def test_package_root_exposes_stable_public_surface() -> None:
    assert hasattr(vertex_forager, "VertexForagerError")
    assert hasattr(vertex_forager, "InputError")
    assert hasattr(vertex_forager, "SchedulerConfig")
    assert hasattr(vertex_forager, "SharadarClient")
    assert hasattr(vertex_forager, "YFinanceClient")
    assert not hasattr(vertex_forager, "DataQualityRule")
    assert not hasattr(vertex_forager, "NoDuplicateRows")
    assert not hasattr(vertex_forager, "NoFutureDates")
    assert not hasattr(vertex_forager, "NoNegativePrices")
    assert not hasattr(vertex_forager, "BaseRouter")
    assert not hasattr(vertex_forager, "create_router")


def test_public_api_module_exposes_expected_surface() -> None:
    assert hasattr(public_api, "create_client")
    assert hasattr(public_api, "StateManager")
    assert hasattr(public_api, "RunResult")
    assert hasattr(public_api, "VertexForagerError")
    assert hasattr(public_api, "InputError")
    assert hasattr(public_api, "SharadarClient")
    assert hasattr(public_api, "YFinanceClient")
    assert not hasattr(public_api, "create_router")
    assert not hasattr(public_api, "BaseRouter")


def test_sharadar_client_exposes_public_async_aliases() -> None:
    assert hasattr(SharadarClient, "get_price_data_async")
    assert hasattr(SharadarClient, "get_fundamental_data_async")
    assert hasattr(SharadarClient, "get_sp500_history_async")
    assert not hasattr(SharadarClient, "_get_price_data_async")
    assert not hasattr(SharadarClient, "_get_fundamental_data_async")
    assert not hasattr(SharadarClient, "_get_sp500_history_async")


def test_yfinance_client_exposes_public_async_aliases() -> None:
    if importlib.util.find_spec("yfinance") is None:
        return
    from vertex_forager.providers.yfinance.client import YFinanceClient

    assert hasattr(YFinanceClient, "get_info_async")
    assert hasattr(YFinanceClient, "get_price_data_async")
    assert hasattr(YFinanceClient, "get_news_async")
    assert not hasattr(YFinanceClient, "_get_info_async")
    assert not hasattr(YFinanceClient, "_get_price_data_async")
    assert not hasattr(YFinanceClient, "_get_news_async")
