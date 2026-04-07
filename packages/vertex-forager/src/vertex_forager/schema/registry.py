"""Central registry for provider dataset and table schemas."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vertex_forager.schema.config import DatasetSpec, TableSchema


@dataclass(frozen=True, slots=True)
class _SchemaRegistry:
    tables: Mapping[str, TableSchema]
    datasets: Mapping[str, Mapping[str, DatasetSpec]]


@lru_cache(maxsize=1)
def _build_registry() -> _SchemaRegistry:
    from vertex_forager.providers.sharadar.schema import DATASETS as SHARADAR_DATASETS
    from vertex_forager.providers.sharadar.schema import TABLES as SHARADAR_TABLES
    from vertex_forager.providers.yfinance.schema import DATASETS as YFINANCE_DATASETS
    from vertex_forager.providers.yfinance.schema import TABLES as YFINANCE_TABLES

    provider_tables = {
        "sharadar": SHARADAR_TABLES,
        "yfinance": YFINANCE_TABLES,
    }
    provider_datasets = {
        "sharadar": SHARADAR_DATASETS,
        "yfinance": YFINANCE_DATASETS,
    }
    tables: dict[str, TableSchema] = {}
    for provider, table_map in provider_tables.items():
        duplicates = sorted(set(tables).intersection(table_map))
        if duplicates:
            raise ValueError(f"Schema table name conflict for provider '{provider}': {duplicates}")
        tables.update(table_map)
    dataset_registry: dict[str, Mapping[str, DatasetSpec]] = {}
    for provider, dataset_specs in provider_datasets.items():
        provider_table_names = set(provider_tables[provider])
        specs_by_name: dict[str, DatasetSpec] = {}
        for spec in dataset_specs:
            if spec.name in specs_by_name:
                raise ValueError(f"Duplicate dataset name '{spec.name}' for provider '{provider}'")
            if spec.schema.table not in provider_table_names:
                raise ValueError(
                    f"DatasetSpec '{provider}.{spec.name}' references unregistered table '{spec.schema.table}'"
                )
            specs_by_name[spec.name] = spec
        dataset_registry[provider] = MappingProxyType(specs_by_name)
    return _SchemaRegistry(
        tables=MappingProxyType(tables),
        datasets=MappingProxyType(dataset_registry),
    )


def get_table_schema(table: str) -> TableSchema | None:
    """Retrieve the schema for a given table name."""
    return _build_registry().tables.get(table)


def get_dataset_spec(provider: str, dataset: str) -> DatasetSpec | None:
    """Retrieve a dataset contract for a provider-local dataset name."""
    return _build_registry().datasets.get(provider, {}).get(dataset)


def get_provider_dataset_names(provider: str) -> tuple[str, ...]:
    """Return all registered dataset names for a provider."""
    return tuple(_build_registry().datasets.get(provider, {}))


def get_provider_dataset_specs(provider: str) -> Mapping[str, DatasetSpec]:
    """Return provider-local dataset specs keyed by dataset name."""
    return _build_registry().datasets.get(provider, MappingProxyType({}))


__all__ = [
    "get_dataset_spec",
    "get_provider_dataset_names",
    "get_provider_dataset_specs",
    "get_table_schema",
]
