"""Built-in provider catalog for schemas and preview constants."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from vertex_forager.schema.config import DatasetSpec, TableSchema


def get_provider_tables() -> dict[str, Mapping[str, TableSchema]]:
    sharadar_schema = import_module("vertex_forager.providers.sharadar.schema")
    yfinance_schema = import_module("vertex_forager.providers.yfinance.schema")

    return {
        "sharadar": sharadar_schema.TABLES,
        "yfinance": yfinance_schema.TABLES,
    }


def get_provider_datasets() -> dict[str, Sequence[DatasetSpec]]:
    sharadar_schema = import_module("vertex_forager.providers.sharadar.schema")
    yfinance_schema = import_module("vertex_forager.providers.yfinance.schema")

    return {
        "sharadar": sharadar_schema.DATASETS,
        "yfinance": yfinance_schema.DATASETS,
    }


def get_provider_constants_preview() -> dict[str, dict[str, object]]:
    sh_constants = import_module("vertex_forager.providers.sharadar.constants")
    yf_constants = import_module("vertex_forager.providers.yfinance.constants")

    return {
        "yfinance": {
            "PRICE_BATCH_SIZE": yf_constants.PRICE_BATCH_SIZE,
            "PRICE_BATCH_MAX": yf_constants.PRICE_BATCH_MAX,
            "THREADS_THRESHOLD": yf_constants.THREADS_THRESHOLD,
            "PRICE_BATCH_SIZE_KEY": yf_constants.PRICE_BATCH_SIZE_KEY,
            "DEFAULT_INTERVAL": yf_constants.DEFAULT_INTERVAL,
            "DEFAULT_PRICE_PERIOD": yf_constants.DEFAULT_PRICE_PERIOD,
        },
        "sharadar": {
            "MAX_ROWS_PER_REQUEST": sh_constants.MAX_ROWS_PER_REQUEST,
            "DEFAULT_BATCH_SIZE": sh_constants.DEFAULT_BATCH_SIZE,
            "MIN_BATCH_SIZE": sh_constants.MIN_BATCH_SIZE,
            "TRADING_DAYS_RATIO": sh_constants.TRADING_DAYS_RATIO,
            "QUARTERLY_DAYS_RATIO": sh_constants.QUARTERLY_DAYS_RATIO,
            "PAGINATION_META_KEY": sh_constants.PAGINATION_META_KEY,
            "PAGINATION_CURSOR_PARAM": sh_constants.PAGINATION_CURSOR_PARAM,
            "MAX_PAGES": sh_constants.MAX_PAGES,
        },
    }


__all__ = [
    "get_provider_constants_preview",
    "get_provider_datasets",
    "get_provider_tables",
]
