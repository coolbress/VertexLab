import importlib
import sys

import pytest


class TestCoreInitLazyImport:
    """Tests for vertex_forager.core lazy-import behavior."""

    def test_from_import_vertexforager_resolves_class(self) -> None:
        from vertex_forager.core import VertexForager

        assert isinstance(VertexForager, type)
        pipeline_module = importlib.import_module("vertex_forager.core.pipeline")
        assert VertexForager is pipeline_module.VertexForager

    def test_module_getattr_vertexforager(self) -> None:
        mod = importlib.import_module("vertex_forager.core")
        attr = mod.VertexForager
        pipeline_module = importlib.import_module("vertex_forager.core.pipeline")
        assert attr is pipeline_module.VertexForager

    def test_missing_attribute_raises_attributeerror(self) -> None:
        mod = importlib.import_module("vertex_forager.core")
        with pytest.raises(AttributeError):
            _ = mod.DoesNotExist

    def test_pipeline_not_prematurely_imported(self) -> None:
        sys.modules.pop("vertex_forager.core.pipeline", None)
        if "vertex_forager.core.pipeline" in sys.modules:
            del sys.modules["vertex_forager.core.pipeline"]
        mod = importlib.import_module("vertex_forager.core")
        if "VertexForager" in mod.__dict__:
            del mod.__dict__["VertexForager"]
        assert "vertex_forager.core.pipeline" not in sys.modules
        assert "VertexForager" not in mod.__dict__
        _ = mod.VertexForager
        assert "vertex_forager.core.pipeline" in sys.modules
