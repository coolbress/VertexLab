"""
Central registry for table schemas.

This module aggregates schema definitions from various providers into a single
lookup table. It serves as the source of truth for `SchemaMapper`.
"""

from __future__ import annotations

from typing import Final

from vertex_forager.schema.config import TableSchema
from vertex_forager.providers.sharadar.schema import TABLES as SHARADAR_TABLES


TABLES: Final[dict[str, TableSchema]] = {
    **SHARADAR_TABLES,
}


def get_table_schema(table: str) -> TableSchema | None:
    """Retrieve the schema for a given table name."""
    return TABLES.get(table)
