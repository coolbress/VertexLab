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
        schema_stub = types.ModuleType("vertex_forager.providers.yfinance.schema")

        class _Client:
            pass

        class _Router:
            pass

        _price_schema = object()
        client_stub.YFinanceClient = _Client
        router_stub.YFinanceRouter = _Router
        schema_stub.YFINANCE_PRICE_SCHEMA = _price_schema
        monkeypatch.setitem(sys.modules, "vertex_forager.providers.yfinance.client", client_stub)
        monkeypatch.setitem(sys.modules, "vertex_forager.providers.yfinance.router", router_stub)
        monkeypatch.setitem(sys.modules, "vertex_forager.providers.yfinance.schema", schema_stub)
        monkeypatch.setattr(mod, "schema", schema_stub, raising=False)

        assert mod.YFinanceClient is _Client
        assert mod.YFinanceRouter is _Router
        assert mod.YFINANCE_PRICE_SCHEMA is _price_schema

    def test_module_getattr_missing_schema_and_unknown_attr(self) -> None:
        mod = importlib.import_module("vertex_forager.providers.yfinance")

        with pytest.raises(AttributeError, match="YFINANCE_NOT_FOUND_SCHEMA"):
            _ = mod.YFINANCE_NOT_FOUND_SCHEMA

        with pytest.raises(AttributeError, match="TOTALLY_UNKNOWN_ATTR"):
            _ = mod.TOTALLY_UNKNOWN_ATTR
