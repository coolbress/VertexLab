from __future__ import annotations

from collections.abc import Callable
import contextlib
from datetime import datetime
import os
from pathlib import Path
from typing import Any

import polars as pl

from vertex_forager.core.config import FramePacket
from vertex_forager.writers.duckdb import DuckDBWriter


def build_summary_message(summary: dict[str, Any]) -> str:
    total_rows_scanned = sum(int(v.get("rows_scanned", 0) or 0) for v in summary["tables"].values())
    total_rows_written = sum(int(v.get("rows_written", 0) or 0) for v in summary["tables"].values())
    ec = summary.get("error_counts", {})
    msg = (
        f"✅ Recover summary: tables={len(summary['tables'])} "
        f"rows_scanned={total_rows_scanned} "
        f"rows_written={total_rows_written} "
        f"errors={len(summary['errors'])}"
    )
    if ec:
        msg += (
            " ("
            f"RecoverFail={ec.get('RecoverFail', 0)} "
            f"CloseFail={ec.get('CloseFail', 0)} "
            f"DeleteFail={ec.get('DeleteFail', 0)})"
        )
    return msg


def aggregate_errors(summary: dict[str, Any]) -> None:
    counts: dict[str, int] = {"RecoverFail": 0, "CloseFail": 0, "DeleteFail": 0}
    for msg in summary["errors"]:
        if isinstance(msg, str):
            if msg.startswith("RecoverFail:"):
                counts["RecoverFail"] += 1
            elif msg.startswith("CloseFail:"):
                counts["CloseFail"] += 1
            elif msg.startswith("DeleteFail:"):
                counts["DeleteFail"] += 1
    summary["error_counts"] = counts


async def scan_table(
    *,
    table: str,
    base_dir: Path,
    progress_flag: bool,
    dry: bool,
    writer: DuckDBWriter | None,
    delete_on_success: bool,
    delete_candidates: dict[str, list[Path]],
    summary: dict[str, Any],
    is_explicit: bool,
    echo: Callable[[str], None],
) -> dict[str, Any] | None:
    table_dir = base_dir / table
    if not table_dir.exists():
        if is_explicit:
            summary["errors"].append(f"RecoverFail:{table}:{table_dir}:missing_table")
        return None
    ipc_files = sorted([p for p in table_dir.glob("batch_*.ipc") if p.is_file()])
    rows_scanned = 0
    rows_written = 0
    file_reports: list[dict[str, Any]] = []
    for file_path in ipc_files:
        try:
            df_rows = 0
            df = pl.read_ipc(file_path)
            df_rows = int(df.height)
            rows_scanned += df_rows
            if progress_flag:
                echo(f"[scan] {table} {file_path.name} rows={df_rows}")
            if dry:
                file_reports.append({"file": str(file_path), "rows": df_rows, "status": "scanned"})
                continue
            packet = FramePacket(
                provider="dlq",
                table=table,
                frame=df,
                observed_at=datetime.now(),
            )
            if writer is None:
                raise RuntimeError("recover: writer is not initialized")
            write_result = await writer.write(packet)
            rows_written += int(write_result.rows)
            file_reports.append({"file": str(file_path), "rows": df_rows, "status": "written"})
            if progress_flag:
                echo(f"[write] {table} {file_path.name} rows={int(write_result.rows)}")
            if delete_on_success:
                delete_candidates.setdefault(table, []).append(file_path)
        except Exception as exc:
            summary["errors"].append(f"RecoverFail:{table}:{file_path}:rows={df_rows}:{exc}")
            file_reports.append({"file": str(file_path), "rows": df_rows, "status": "failed", "error": str(exc)})
    return {
        "files_scanned": len(ipc_files),
        "rows_scanned": rows_scanned,
        "rows_written": rows_written,
        "details": file_reports,
    }


def _mark_delete_status(
    *,
    summary: dict[str, Any],
    table_name: str,
    file_path: Path,
    from_status: str,
    to_status: str,
) -> None:
    details = summary["tables"].get(table_name, {}).get("details", [])
    for entry in details:
        if entry.get("file") == str(file_path) and entry.get("status") == from_status:
            entry["status"] = to_status


def _process_successful_deletions(
    *,
    delete_candidates: dict[str, list[Path]],
    summary: dict[str, Any],
) -> None:
    for table_name, files in delete_candidates.items():
        for file_path in files:
            try:
                file_path.unlink()
                with contextlib.suppress(Exception):
                    dir_fd = os.open(str(file_path.parent), os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                _mark_delete_status(
                    summary=summary,
                    table_name=table_name,
                    file_path=file_path,
                    from_status="written",
                    to_status="deleted",
                )
            except Exception as exc:
                summary["errors"].append(f"DeleteFail:{table_name}:{file_path}:{exc}")
                _mark_delete_status(
                    summary=summary,
                    table_name=table_name,
                    file_path=file_path,
                    from_status="written",
                    to_status="delete_failed",
                )


def _mark_close_failed_deletions(*, delete_candidates: dict[str, list[Path]], summary: dict[str, Any]) -> None:
    for table_name, files in delete_candidates.items():
        for file_path in files:
            _mark_delete_status(
                summary=summary,
                table_name=table_name,
                file_path=file_path,
                from_status="written",
                to_status="close_failed",
            )


def process_deletions(
    *,
    delete_candidates: dict[str, list[Path]],
    summary: dict[str, Any],
    delete_on_success: bool,
    dry_run: bool,
    writer_closed_ok: bool,
) -> None:
    if delete_on_success and not dry_run and writer_closed_ok:
        _process_successful_deletions(delete_candidates=delete_candidates, summary=summary)
    if not writer_closed_ok:
        _mark_close_failed_deletions(delete_candidates=delete_candidates, summary=summary)


async def execute_recovery(
    *,
    base: Path,
    selected_tables: list[str],
    explicit_tables: set[str],
    target_db: Path | None,
    dry_run: bool,
    delete_on_success: bool,
    progress: bool,
    summary: dict[str, Any],
    echo: Callable[[str], None],
    logger: Any,
) -> None:
    writer: DuckDBWriter | None = None
    delete_candidates: dict[str, list[Path]] = {}
    try:
        if not dry_run:
            if target_db is None:
                raise RuntimeError("recover: missing target DB")
            writer = DuckDBWriter(target_db)
        for table_name in selected_tables:
            table_report = await scan_table(
                table=table_name,
                base_dir=base,
                progress_flag=progress,
                dry=dry_run,
                writer=writer,
                delete_on_success=delete_on_success,
                delete_candidates=delete_candidates,
                summary=summary,
                is_explicit=table_name in explicit_tables,
                echo=echo,
            )
            if table_report is not None:
                summary["tables"][table_name] = table_report
    finally:
        writer_closed_ok = True
        if writer is not None:
            try:
                await writer.close()
            except Exception as exc:
                writer_closed_ok = False
                summary["errors"].append(f"CloseFail:writer:{exc}")
                logger.warning("Recover: writer close failed: %s", exc)
        process_deletions(
            delete_candidates=delete_candidates,
            summary=summary,
            delete_on_success=delete_on_success,
            dry_run=dry_run,
            writer_closed_ok=writer_closed_ok,
        )
    aggregate_errors(summary)
