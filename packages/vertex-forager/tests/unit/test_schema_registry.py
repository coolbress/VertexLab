from __future__ import annotations

from types import MappingProxyType

import pytest

import vertex_forager.providers.catalog as provider_catalog
from vertex_forager.schema import registry as schema_registry


def test_build_registry_rejects_provider_key_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    schema_registry._build_registry.cache_clear()
    monkeypatch.setattr(
        provider_catalog,
        "get_provider_tables",
        lambda: {"sharadar": MappingProxyType({})},
    )
    monkeypatch.setattr(
        provider_catalog,
        "get_provider_datasets",
        lambda: {"yfinance": ()},
    )

    with pytest.raises(ValueError, match="Provider registry key mismatch"):
        schema_registry._build_registry()

    schema_registry._build_registry.cache_clear()
