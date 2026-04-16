from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
import json
from typing import Any, cast

import polars as pl
from pydantic import BaseModel, Field, field_serializer, field_validator

from vertex_forager.core.config import RequestSpec
from vertex_forager.core.types import JSONValue
from vertex_forager.exceptions import RunError


def _normalize_json_value(value: JSONValue) -> JSONValue:
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_json_value(val) for key, val in value.items()}
    return value


@dataclass(frozen=True, slots=True)
class FetchJob:
    """Unit of work for the fetch pipeline."""

    provider: str
    dataset: str
    symbol: str | None = None
    spec: RequestSpec = field(default_factory=lambda: RequestSpec(url=""))
    context: Mapping[str, JSONValue] = field(default_factory=dict)
    _signature: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "context", dict(self.context))
        object.__setattr__(self, "_signature", self._compute_signature())

    @property
    def signature(self) -> str:
        return self._signature

    def __hash__(self) -> int:
        return hash(self._signature)

    def _compute_signature(self) -> str:
        payload = {
            "provider": self.provider,
            "dataset": self.dataset,
            "symbol": self.symbol,
            "spec": self.spec.model_dump(mode="json"),
            "context": dict(self.context),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)

    def to_checkpoint_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "dataset": self.dataset,
            "symbol": self.symbol,
            "spec": self.spec.model_dump(mode="json"),
            "context": _normalize_json_value(dict(self.context)),
        }

    @classmethod
    def from_checkpoint_dict(cls, data: Mapping[str, object]) -> FetchJob:
        context = data.get("context", {})
        return cls(
            provider=str(data["provider"]),
            dataset=str(data["dataset"]),
            symbol=str(data["symbol"]) if data.get("symbol") is not None else None,
            spec=RequestSpec.model_validate(data["spec"]),
            context=context if isinstance(context, Mapping) else {},
        )

    def model_copy(self, *, update: Mapping[str, object] | None = None, deep: bool = False) -> FetchJob:
        del deep
        values = dict(update or {})
        return FetchJob(
            provider=cast(str, values.get("provider", self.provider)),
            dataset=cast(str, values.get("dataset", self.dataset)),
            symbol=cast(str | None, values.get("symbol", self.symbol)),
            spec=cast(RequestSpec, values.get("spec", self.spec)),
            context=cast(Mapping[str, JSONValue], values.get("context", self.context)),
        )


@dataclass(frozen=True, slots=True, eq=False)
class FramePacket:
    """Polars frame packet passed from provider to sink."""

    provider: str
    table: str
    frame: pl.DataFrame
    observed_at: datetime
    partition_date: date | None = None
    context: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "context", dict(self.context))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FramePacket):
            return NotImplemented
        return (
            self.provider == other.provider
            and self.table == other.table
            and self.observed_at == other.observed_at
            and self.partition_date == other.partition_date
            and dict(self.context) == dict(other.context)
            and self.frame.equals(other.frame)
        )

    def model_copy(self, *, update: Mapping[str, object] | None = None, deep: bool = False) -> FramePacket:
        del deep
        values = dict(update or {})
        return FramePacket(
            provider=cast(str, values.get("provider", self.provider)),
            table=cast(str, values.get("table", self.table)),
            frame=cast(pl.DataFrame, values.get("frame", self.frame)),
            observed_at=cast(datetime, values.get("observed_at", self.observed_at)),
            partition_date=cast(date | None, values.get("partition_date", self.partition_date)),
            context=cast(Mapping[str, JSONValue], values.get("context", self.context)),
        )


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
