from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import polars as pl

from vertex_forager.core.checkpoint import delete_dlq_entries, list_dlq_entries, mark_dlq_retry_result
from vertex_forager.core.config import FramePacket
from vertex_forager.utils import run_sync_compat
from vertex_forager.writers import create_writer
from vertex_forager.writers.base import BaseWriter
from vertex_forager.writers.memory import InMemoryBufferWriter


def _resolve_replay_entry(row: Mapping[str, object]) -> tuple[Path, str]:
    path = Path(str(row["path"]))
    provider = row.get("provider")
    if isinstance(provider, str) and provider:
        return path, provider
    return path, str(row["table"]).split("_", 1)[0]


def _summarize_dry_run(rows: Sequence[Mapping[str, object]]) -> ReplayResult:
    replayable = 0
    skipped = 0
    for row in rows:
        path, _provider = _resolve_replay_entry(row)
        if not path.exists():
            skipped += 1
        else:
            replayable += 1
    return ReplayResult(replayed=replayable, failed=0, skipped=skipped, errors=[])


async def _write_replay_row(
    *,
    provider: str,
    row: Mapping[str, object],
    path: Path,
    writer: BaseWriter,
) -> str | None:
    frame = pl.read_ipc(path)
    await writer.write(
        FramePacket(
            provider=provider,
            table=str(row["table"]),
            frame=frame,
            observed_at=datetime.now(timezone.utc),
        )
    )
    return None


@dataclass(frozen=True, slots=True)
class DLQEntry:
    provider: str | None
    table: str
    row_count: int
    retry_count: int
    status: str
    created_at: datetime
    path: Path


@dataclass(frozen=True, slots=True)
class ReplayResult:
    replayed: int
    failed: int
    skipped: int
    errors: list[str]


class DLQNamespace:
    def list(
        self,
        *,
        table: str | None = None,
        status: str = "pending",
    ) -> list[DLQEntry]:
        rows = list_dlq_entries(table=table, status=status)
        return [
            DLQEntry(
                provider=str(row["provider"]) if row["provider"] is not None else None,
                table=str(row["table"]),
                row_count=int(cast("int | float | str", row["row_count"])),
                retry_count=int(cast("int | float | str", row["retry_count"])),
                status=str(row["status"]),
                created_at=datetime.fromtimestamp(float(cast("int | float | str", row["created_at"])), tz=timezone.utc),
                path=Path(str(row["path"])),
            )
            for row in rows
        ]

    def clear(
        self,
        *,
        table: str | None = None,
    ) -> int:
        result = delete_dlq_entries(table=table)
        return int(result["rows"])

    def replay(
        self,
        *,
        table: str,
        output: str,
        dry_run: bool = False,
    ) -> ReplayResult:
        return run_sync_compat(self._replay_async(table=table, output=output, dry_run=dry_run))

    async def _replay_async(
        self,
        *,
        table: str,
        output: str,
        dry_run: bool,
    ) -> ReplayResult:
        rows = list_dlq_entries(table=table, status="pending")
        if dry_run:
            return _summarize_dry_run(rows)

        writer_factory = create_writer(output)
        if isinstance(writer_factory, InMemoryBufferWriter):
            raise ValueError("DLQ replay requires a persisted output destination")
        replayed = 0
        failed = 0
        skipped = 0
        errors: list[str] = []
        async with AsyncExitStack() as stack:
            writer = await stack.enter_async_context(writer_factory)
            for row in rows:
                path, provider = _resolve_replay_entry(row)
                if not path.exists():
                    skipped += 1
                    errors.append(f"Missing DLQ payload: {path}")
                    mark_dlq_retry_result(path=path, success=False, error="missing payload")
                    continue
                try:
                    error = await _write_replay_row(
                        provider=provider,
                        row=row,
                        path=path,
                        writer=writer,
                    )
                    if error is not None:
                        failed += 1
                        errors.append(error)
                        mark_dlq_retry_result(path=path, success=False, error=error)
                        continue
                    mark_dlq_retry_result(path=path, success=True)
                    replayed += 1
                except Exception as exc:
                    failed += 1
                    errors.append(f"{path}: {exc}")
                    mark_dlq_retry_result(path=path, success=False, error=str(exc))
        return ReplayResult(replayed=replayed, failed=failed, skipped=skipped, errors=errors)
