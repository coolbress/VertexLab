from __future__ import annotations

import importlib
import sys
import types

import pytest


class TestYFinanceInitLazyExport:
    def test_module_getattr_resolves_client_router_and_schema(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mod = importlib.import_module("vertex_forager.providers.yfinance")
        client_stub = types.ModuleType("vertex_forager.providers.yfinance.client")
        router_stub = types.ModuleType("vertex_forager.providers.yfinance.router")

        class _Client:
            pass

        class _Router:
            pass

        client_stub.YFinanceClient = _Client
        router_stub.YFinanceRouter = _Router
        monkeypatch.setitem(sys.modules, "vertex_forager.providers.yfinance.client", client_stub)
        monkeypatch.setitem(sys.modules, "vertex_forager.providers.yfinance.router", router_stub)

        assert mod.YFinanceClient is _Client
        assert mod.YFinanceRouter is _Router
        assert mod.YFINANCE_PRICE_SCHEMA is not None

    def test_module_getattr_missing_schema_and_unknown_attr(self) -> None:
        mod = importlib.import_module("vertex_forager.providers.yfinance")

        with pytest.raises(AttributeError, match="YFINANCE_NOT_FOUND_SCHEMA"):
            _ = mod.YFINANCE_NOT_FOUND_SCHEMA

        with pytest.raises(AttributeError, match="TOTALLY_UNKNOWN_ATTR"):
            _ = mod.TOTALLY_UNKNOWN_ATTR
