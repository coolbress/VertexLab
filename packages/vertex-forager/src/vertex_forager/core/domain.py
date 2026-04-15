from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import polars as pl
from pydantic import BaseModel, Field, field_serializer, field_validator

from vertex_forager.core.config import RequestSpec
from vertex_forager.core.types import JSONValue
from vertex_forager.exceptions import RunError


class FetchJob(BaseModel):
    """Unit of work for the fetch pipeline."""

    provider: str
    dataset: str
    symbol: str | None = None
    spec: RequestSpec
    context: Mapping[str, JSONValue] = Field(default_factory=dict)


class FramePacket(BaseModel):
    """Polars frame packet passed from provider to sink."""

    provider: str
    table: str
    frame: pl.DataFrame
    observed_at: datetime
    partition_date: date | None = None
    context: Mapping[str, JSONValue] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}


class RunResult(BaseModel):
    """Result summary for a pipeline run."""

    provider: str
    run_id: str | None = Field(default=None)
    dataset: str | None = Field(default=None)
    started_at: float | None = Field(default=None)
    finished_at: float | None = Field(default=None)
    duration_s: float | None = Field(default=None)
    coverage_pct: float | None = Field(default=None)
    tables: dict[str, int] = Field(default_factory=dict)
    errors: list[RunError] = Field(default_factory=list)
    data: pl.DataFrame | None = Field(default=None)
    metrics_counters: dict[str, int] = Field(default_factory=dict, exclude=True)
    metrics_histograms: dict[str, list[float]] = Field(default_factory=dict, exclude=True)
    metrics_summary: dict[str, float] = Field(default_factory=dict)
    dlq_pending: dict[str, list[FramePacket]] = Field(
        default_factory=dict,
        exclude=True,
        description="Packets preserved per table for post-mortem DLQ handling when spool/dispatch fails",
    )
    dlq_counts: dict[str, dict[str, int]] = Field(
        default_factory=dict,
        description="Per-table DLQ counts: {'rescued': int, 'remaining': int}",
    )
    quality_violations: dict[str, int] = Field(
        default_factory=dict,
        description="Per-table quality violation counts",
    )

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("errors", mode="before")
    @classmethod
    def _coerce_string_errors(cls, v: Any) -> Any:
        if not isinstance(v, (list, tuple)):
            return v
        result: list[Any] = []
        for item in v:
            if isinstance(item, str):
                result.append(
                    RunError(
                        provider="",
                        dataset="",
                        symbol="",
                        exc_type="builtins.str",
                        message=item,
                        retryable=False,
                    )
                )
            else:
                result.append(item)
        return result

    @field_serializer("data", when_used="json")
    def _serialize_data_for_json(self, value: pl.DataFrame | None) -> list[dict[str, Any]] | None:
        if value is None:
            return None
        return value.to_dicts()

    def add_rows(self, *, table: str, rows: int) -> None:
        self.tables[table] = self.tables.get(table, 0) + rows

    def add_quality_violations(self, *, table: str, count: int) -> None:
        self.quality_violations[table] = self.quality_violations.get(table, 0) + count


@dataclass(frozen=True, slots=True)
class ParseResult:
    """Result of parsing a response."""

    packets: list[FramePacket]
    next_jobs: list[FetchJob]


__all__ = [
    "FetchJob",
    "FramePacket",
    "ParseResult",
    "RunResult",
]
