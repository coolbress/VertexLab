from __future__ import annotations

from dataclasses import dataclass
import time
from typing import cast

from vertex_forager.core.checkpoint import delete_run_history, list_run_history


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    provider: str
    dataset: str | None
    table_name: str | None
    started_at: float | None
    finished_at: float | None
    duration_s: float | None
    tables: dict[str, int]
    error_count: int
    errors: list[dict[str, str]]
    quality_violations: dict[str, int]
    coverage_pct: float | None
    created_at: float


class RunsNamespace:
    def list(
        self,
        *,
        table: str | None = None,
        limit: int = 20,
    ) -> list[RunRecord]:
        rows = list_run_history(limit=limit, table_name=table)
        records: list[RunRecord] = []
        for row in rows:
            tables_raw = row.get("tables")
            errors_raw = row.get("errors")
            violations_raw = row.get("quality_violations")
            tables = {str(key): int(value) for key, value in tables_raw.items()} if isinstance(tables_raw, dict) else {}
            errors = (
                [{str(k): str(v) for k, v in item.items()} for item in errors_raw if isinstance(item, dict)]
                if isinstance(errors_raw, list)
                else []
            )
            quality_violations = (
                {str(key): int(value) for key, value in violations_raw.items()}
                if isinstance(violations_raw, dict)
                else {}
            )
            records.append(
                RunRecord(
                    run_id=str(row["run_id"]),
                    provider=str(row["provider"]),
                    dataset=str(row["dataset"]) if row["dataset"] is not None else None,
                    table_name=str(row["table_name"]) if row["table_name"] is not None else None,
                    started_at=(
                        float(cast("int | float | str", row["started_at"])) if row["started_at"] is not None else None
                    ),
                    finished_at=(
                        float(cast("int | float | str", row["finished_at"])) if row["finished_at"] is not None else None
                    ),
                    duration_s=(
                        float(cast("int | float | str", row["duration_s"])) if row["duration_s"] is not None else None
                    ),
                    tables=tables,
                    error_count=int(cast("int | float | str", row["error_count"])),
                    errors=errors,
                    quality_violations=quality_violations,
                    coverage_pct=(
                        float(cast("int | float | str", row["coverage_pct"]))
                        if row["coverage_pct"] is not None
                        else None
                    ),
                    created_at=float(cast("int | float | str", row["created_at"])),
                )
            )
        return records

    def clear(
        self,
        *,
        table: str | None = None,
        before_days: int | None = None,
    ) -> int:
        before_ts = None
        if before_days is not None:
            days = int(before_days)
            if days < 0:
                raise ValueError("before_days must be non-negative")
            before_ts = time.time() - (days * 86_400)
        return delete_run_history(table_name=table, before_ts=before_ts)
