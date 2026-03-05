from __future__ import annotations

import importlib


def test_logging_constants_import() -> None:
    mod = importlib.import_module("vertex_forager.logging.constants")
    assert hasattr(mod, "ROUTER_LOG_PREFIX")
    assert hasattr(mod, "CLIENT_LOG_PREFIX")
