import importlib
import sys

import pytest


class TestSharadarInitLazyExport:
    """Tests for vertex_forager.providers.sharadar lazy-export behavior."""
    def test_from_import_client_router_resolve_classes(self) -> None:
        from vertex_forager.providers.sharadar import SharadarClient, SharadarRouter
        client_module = importlib.import_module(
            "vertex_forager.providers.sharadar.client"
        )
        router_module = importlib.import_module(
            "vertex_forager.providers.sharadar.router"
        )
        assert SharadarClient is client_module.SharadarClient
        assert SharadarRouter is router_module.SharadarRouter

    def test_module_getattr_client_router(self) -> None:
        mod = importlib.import_module("vertex_forager.providers.sharadar")
        client_module = importlib.import_module(
            "vertex_forager.providers.sharadar.client"
        )
        router_module = importlib.import_module(
            "vertex_forager.providers.sharadar.router"
        )
        assert mod.SharadarClient is client_module.SharadarClient
        assert mod.SharadarRouter is router_module.SharadarRouter

    def test_missing_attribute_raises_attributeerror(self) -> None:
        mod = importlib.import_module("vertex_forager.providers.sharadar")
        with pytest.raises(AttributeError):
            _ = mod.DoesNotExist

    def test_modules_not_prematurely_imported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(
            sys.modules, "vertex_forager.providers.sharadar.client", raising=False
        )
        monkeypatch.delitem(
            sys.modules, "vertex_forager.providers.sharadar.router", raising=False
        )
        assert "vertex_forager.providers.sharadar.client" not in sys.modules
        assert "vertex_forager.providers.sharadar.router" not in sys.modules
        mod = importlib.import_module("vertex_forager.providers.sharadar")
        for attr in ("SharadarClient", "SharadarRouter"):
            monkeypatch.delitem(mod.__dict__, attr, raising=False)
        _ = mod.SharadarClient
        _ = mod.SharadarRouter
        assert "vertex_forager.providers.sharadar.client" in sys.modules
        assert "vertex_forager.providers.sharadar.router" in sys.modules
