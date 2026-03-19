"""
Central registry for table schemas.

This module aggregates schema definitions from various providers into a single
lookup table. It serves as the source of truth for `SchemaMapper`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from vertex_forager.providers.sharadar.schema import TABLES as SHARADAR_TABLES
from vertex_forager.providers.yfinance.schema import TABLES as YFINANCE_TABLES

if TYPE_CHECKING:
    from vertex_forager.schema.config import TableSchema

_sh_keys = set(SHARADAR_TABLES.keys())
_yf_keys = set(YFINANCE_TABLES.keys())
_dup = _sh_keys & _yf_keys
if _dup:
    raise ValueError(f"Schema table name conflict: {sorted(_dup)}")
TABLES: Final[dict[str, TableSchema]] = {**SHARADAR_TABLES, **YFINANCE_TABLES}


def get_table_schema(table: str) -> TableSchema | None:
    """Retrieve the schema for a given table name."""
    return TABLES.get(table)
