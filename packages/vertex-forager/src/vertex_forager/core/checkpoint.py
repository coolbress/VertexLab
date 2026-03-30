"""SQLite-backed persistence utilities for checkpoints, runs, and DLQ state."""

from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel, Field, ValidationError

if TYPE_CHECKING:
    from vertex_forager.core.config import RunResult
    from vertex_forager.core.types import JSONValue

from vertex_forager.core.config import FetchJob
from vertex_forager.core.errors import RunError


def get_cache_dir() -> Path:
    """Get the cache directory following XDG Base Directory spec.

    If VERTEXFORAGER_ROOT environment variable is set, uses that as the base directory
    for compatibility with test environments.

    Returns:
        Path: Cache directory path.
    """
    vertex_root = os.environ.get("VERTEXFORAGER_ROOT")
    if vertex_root:
        cache_dir = Path(vertex_root) / "cache"
    else:
        cache_home = os.environ.get("XDG_CACHE_HOME")
        cache_dir = Path(cache_home) / "vertex-forager" if cache_home else Path.home() / ".cache" / "vertex-forager"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_state_db_path() -> Path:
    """Return the SQLite state database path."""
    return get_cache_dir() / "state.db"


class Checkpoint(BaseModel):
    """Checkpoint data model for resumable runs."""

    run_id: str
    provider: str
    dataset: str
    completed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    pending_jobs: list[FetchJob] = Field(default_factory=list)
    status: Literal["in_progress", "completed"] = "in_progress"


def _connect_state_db() -> sqlite3.Connection:
    """Open the state database and ensure the schema exists."""
    db_path = get_state_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _initialize_schema(conn)
    return conn


def _initialize_schema(conn: sqlite3.Connection) -> None:
    """Create SQLite tables required for local pipeline state."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS checkpoints (
            run_id TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            dataset TEXT NOT NULL,
            completed_json TEXT NOT NULL,
            failed_json TEXT NOT NULL,
            pending_jobs_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'in_progress',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_checkpoints_provider_dataset_updated
            ON checkpoints(provider, dataset, updated_at DESC);

        CREATE TABLE IF NOT EXISTS run_history (
            run_id TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            dataset TEXT,
            started_at REAL,
            finished_at REAL,
            duration_s REAL,
            tables_json TEXT NOT NULL,
            error_count INTEGER NOT NULL,
            errors_json TEXT NOT NULL,
            quality_violations_json TEXT NOT NULL,
            coverage_pct REAL,
            created_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_run_history_created_at
            ON run_history(created_at DESC);

        CREATE TABLE IF NOT EXISTS dlq_index (
            path TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            provider TEXT,
            row_count INTEGER NOT NULL,
            created_at REAL NOT NULL,
            status TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_retried_at REAL
        );

        CREATE INDEX IF NOT EXISTS idx_dlq_index_status_table_created
            ON dlq_index(status, table_name, created_at DESC);
        """
    )
    checkpoint_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(checkpoints)").fetchall()}
    if "pending_jobs_json" not in checkpoint_columns:
        conn.execute("ALTER TABLE checkpoints ADD COLUMN pending_jobs_json TEXT NOT NULL DEFAULT '[]'")
    conn.commit()


def _json_dumps(value: object) -> str:
    """Serialize a value to compact JSON text."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _json_loads(value: str | None, default: JSONValue) -> JSONValue:
    """Deserialize JSON text and fall back to the provided default."""
    if value is None or value == "":
        return default
    try:
        return cast("JSONValue", json.loads(value))
    except json.JSONDecodeError:
        return default


def _serialize_run_error(error: object) -> dict[str, JSONValue]:
    """Convert a run error object to a JSON-serializable dictionary."""
    if isinstance(error, RunError):
        return {
            "provider": error.provider,
            "dataset": error.dataset,
            "symbol": error.symbol,
            "exc_type": error.exc_type,
            "message": error.message,
            "retryable": error.retryable,
        }
    return {
        "provider": "",
        "dataset": "",
        "symbol": "",
        "exc_type": f"{type(error).__module__}.{type(error).__name__}",
        "message": str(error),
        "retryable": False,
    }


def _build_run_history_payload(run_result: RunResult, run_id: str) -> dict[str, JSONValue]:
    """Build a serializable run-history payload."""
    return {
        "run_id": run_id,
        "provider": run_result.provider,
        "dataset": run_result.dataset,
        "started_at": run_result.started_at,
        "finished_at": run_result.finished_at,
        "duration_s": run_result.duration_s,
        "tables": dict(run_result.tables),
        "error_count": len(run_result.errors),
        "errors": [_serialize_run_error(error) for error in run_result.errors],
        "quality_violations": {str(table): int(count) for table, count in run_result.quality_violations.items()},
        "coverage_pct": run_result.coverage_pct,
    }


def save_checkpoint(checkpoint: Checkpoint) -> None:
    """Save checkpoint state to SQLite.

    Args:
        checkpoint: Checkpoint data to save.
    """
    now = time.time()
    with closing(_connect_state_db()) as conn:
        conn.execute(
            """
            INSERT INTO checkpoints (
                run_id,
                provider,
                dataset,
                completed_json,
                failed_json,
                pending_jobs_json,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                provider=excluded.provider,
                dataset=excluded.dataset,
                completed_json=excluded.completed_json,
                failed_json=excluded.failed_json,
                pending_jobs_json=excluded.pending_jobs_json,
                status=excluded.status,
                updated_at=excluded.updated_at
            """,
            (
                checkpoint.run_id,
                checkpoint.provider,
                checkpoint.dataset,
                _json_dumps(checkpoint.completed),
                _json_dumps(checkpoint.failed),
                _json_dumps([job.model_dump(mode="json") for job in checkpoint.pending_jobs]),
                checkpoint.status,
                now,
                now,
            ),
        )
        conn.commit()


def load_checkpoint(run_id: str) -> Checkpoint | None:
    """Load checkpoint state from SQLite.

    Args:
        run_id: Run identifier.

    Returns:
        Checkpoint if present and valid, otherwise None.
    """
    with closing(_connect_state_db()) as conn:
        row = conn.execute(
            """
            SELECT run_id, provider, dataset, completed_json, failed_json, pending_jobs_json, status
            FROM checkpoints
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    if row is None:
        return None
    try:
        return Checkpoint(
            run_id=str(row["run_id"]),
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            completed=list(cast("list[str]", _json_loads(str(row["completed_json"]), []))),
            failed=list(cast("list[str]", _json_loads(str(row["failed_json"]), []))),
            pending_jobs=[
                FetchJob.model_validate(item)
                for item in cast("list[object]", _json_loads(str(row["pending_jobs_json"]), []))
                if isinstance(item, dict)
            ],
            status=str(row["status"]) if row["status"] else "in_progress",
        )
    except (TypeError, ValueError, ValidationError):
        return None


def find_latest_checkpoint(provider: str, dataset: str) -> Checkpoint | None:
    """Find the latest checkpoint for a provider and dataset pair."""
    with closing(_connect_state_db()) as conn:
        row = conn.execute(
            """
            SELECT run_id, provider, dataset, completed_json, failed_json, pending_jobs_json, status
            FROM checkpoints
            WHERE provider = ? AND dataset = ? AND status != 'completed'
            ORDER BY updated_at DESC, rowid DESC
            LIMIT 1
            """,
            (provider, dataset),
        ).fetchone()
    if row is None:
        return None
    try:
        return Checkpoint(
            run_id=str(row["run_id"]),
            provider=str(row["provider"]),
            dataset=str(row["dataset"]),
            completed=list(cast("list[str]", _json_loads(str(row["completed_json"]), []))),
            failed=list(cast("list[str]", _json_loads(str(row["failed_json"]), []))),
            pending_jobs=[
                FetchJob.model_validate(item)
                for item in cast("list[object]", _json_loads(str(row["pending_jobs_json"]), []))
                if isinstance(item, dict)
            ],
            status=str(row["status"]) if row["status"] else "in_progress",
        )
    except (TypeError, ValueError, ValidationError):
        return None


def save_run_history(run_result: RunResult, run_id: str) -> None:
    """Save run history to SQLite.

    Args:
        run_result: Run result to persist.
        run_id: Run identifier.
    """
    payload = _build_run_history_payload(run_result, run_id)
    created_at = float(run_result.finished_at or run_result.started_at or time.time())
    with closing(_connect_state_db()) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO run_history (
                run_id,
                provider,
                dataset,
                started_at,
                finished_at,
                duration_s,
                tables_json,
                error_count,
                errors_json,
                quality_violations_json,
                coverage_pct,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload["run_id"]),
                str(payload["provider"]),
                payload["dataset"],
                payload["started_at"],
                payload["finished_at"],
                payload["duration_s"],
                _json_dumps(payload["tables"]),
                int(cast("int", payload["error_count"])),
                _json_dumps(payload["errors"]),
                _json_dumps(payload["quality_violations"]),
                payload["coverage_pct"],
                created_at,
            ),
        )
        conn.commit()


def list_run_history(limit: int = 20) -> list[dict[str, JSONValue]]:
    """List recent run-history records ordered by newest first."""
    row_limit = max(1, int(limit))
    with closing(_connect_state_db()) as conn:
        rows = conn.execute(
            """
            SELECT
                run_id,
                provider,
                dataset,
                started_at,
                finished_at,
                duration_s,
                tables_json,
                error_count,
                quality_violations_json,
                coverage_pct,
                created_at
            FROM run_history
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (row_limit,),
        ).fetchall()
    results: list[dict[str, JSONValue]] = []
    for row in rows:
        tables = _json_loads(str(row["tables_json"]), {})
        quality_violations = _json_loads(str(row["quality_violations_json"]), {})
        total_rows = sum(int(value) for value in dict(tables).values()) if isinstance(tables, dict) else 0
        results.append(
            {
                "run_id": str(row["run_id"]),
                "provider": str(row["provider"]),
                "dataset": row["dataset"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "duration_s": row["duration_s"],
                "error_count": int(row["error_count"]),
                "total_rows": total_rows,
                "coverage_pct": row["coverage_pct"],
                "quality_violations": quality_violations,
                "created_at": row["created_at"],
            }
        )
    return results


def delete_run_history_before(before_ts: float) -> int:
    """Delete run-history rows older than the provided timestamp."""
    with closing(_connect_state_db()) as conn:
        cursor = conn.execute("DELETE FROM run_history WHERE created_at < ?", (float(before_ts),))
        conn.commit()
        return int(cursor.rowcount)


def delete_all_run_history() -> int:
    """Delete all run-history rows."""
    with closing(_connect_state_db()) as conn:
        cursor = conn.execute("DELETE FROM run_history")
        conn.commit()
        return int(cursor.rowcount)


def delete_completed_checkpoints_before(before_ts: float) -> int:
    """Delete completed checkpoint rows older than the provided timestamp."""
    with closing(_connect_state_db()) as conn:
        cursor = conn.execute(
            "DELETE FROM checkpoints WHERE status = 'completed' AND updated_at < ?",
            (float(before_ts),),
        )
        conn.commit()
        return int(cursor.rowcount)


def delete_all_checkpoints() -> int:
    """Delete all checkpoint rows."""
    with closing(_connect_state_db()) as conn:
        cursor = conn.execute("DELETE FROM checkpoints")
        conn.commit()
        return int(cursor.rowcount)


def register_dlq_entry(*, path: Path, table: str, provider: str | None, row_count: int) -> None:
    """Register a spooled DLQ file in the SQLite index."""
    dlq_root = get_cache_dir().resolve() / "dlq"
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(dlq_root)
    except ValueError:
        raise ValueError(f"DLQ path {path} is outside DLQ root {dlq_root}") from None
    created_at = time.time()
    with closing(_connect_state_db()) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO dlq_index (
                path,
                table_name,
                provider,
                row_count,
                created_at,
                status,
                retry_count,
                last_error,
                last_retried_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', 0, NULL, NULL)
            """,
            (
                str(resolved_path),
                table,
                provider,
                int(row_count),
                created_at,
            ),
        )
        conn.commit()


def list_pending_dlq_entries(table: str | None = None) -> list[dict[str, JSONValue]]:
    """List pending DLQ entries, optionally filtered by table."""
    query = """
        SELECT path, table_name, provider, row_count, created_at, status, retry_count, last_error, last_retried_at
        FROM dlq_index
        WHERE status = 'pending'
    """
    params: tuple[object, ...]
    if table:
        query += " AND table_name = ?"
        params = (table,)
    else:
        params = ()
    query += " ORDER BY created_at DESC"
    with closing(_connect_state_db()) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "path": str(row["path"]),
            "table": str(row["table_name"]),
            "provider": row["provider"],
            "row_count": int(row["row_count"]),
            "created_at": float(row["created_at"]),
            "status": str(row["status"]),
            "retry_count": int(row["retry_count"]),
            "last_error": row["last_error"],
            "last_retried_at": row["last_retried_at"],
        }
        for row in rows
    ]


def mark_dlq_retry_result(*, path: Path, success: bool, error: str | None = None) -> None:
    """Update DLQ index status after a retry attempt."""
    status = "recovered" if success else "pending"
    with closing(_connect_state_db()) as conn:
        conn.execute(
            """
            UPDATE dlq_index
            SET
                status = ?,
                retry_count = retry_count + 1,
                last_error = ?,
                last_retried_at = ?
            WHERE path = ?
            """,
            (
                status,
                error,
                time.time(),
                str(path),
            ),
        )
        conn.commit()


def delete_dlq_entry(path: Path) -> bool:
    """Delete a DLQ index entry and its payload file by spool path."""
    resolved_path = path.resolve()
    dlq_root = get_cache_dir().resolve() / "dlq"
    if not resolved_path.is_relative_to(dlq_root):
        raise ValueError(f"DLQ path {path} is outside DLQ root {dlq_root}") from None
    payload_path = resolved_path if resolved_path.suffix == ".ipc" else Path(f"{resolved_path}.ipc")
    try:
        if payload_path.exists():
            payload_path.unlink(missing_ok=True)
    except OSError:
        return False
    with closing(_connect_state_db()) as conn:
        cursor = conn.execute("DELETE FROM dlq_index WHERE path = ?", (str(resolved_path),))
        conn.commit()
        return int(cursor.rowcount) > 0


def _remove_dlq_files(paths: list[Path]) -> int:
    """Delete DLQ IPC files and return the number of successfully removed files."""
    deleted_files = 0
    dlq_root = get_cache_dir().resolve() / "dlq"
    for path in paths:
        try:
            resolved = path.resolve()
            resolved.relative_to(dlq_root)
        except ValueError:
            continue
        try:
            if path.exists():
                path.unlink(missing_ok=True)
                deleted_files += 1
        except OSError:
            continue
        parent = path.parent
        while parent != dlq_root and parent.exists():
            try:
                next(parent.iterdir())
                break
            except StopIteration:
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
            except OSError:
                break
    return deleted_files


def delete_dlq_entries_before(before_ts: float) -> dict[str, int]:
    """Delete old DLQ index rows and their IPC payload files."""
    with closing(_connect_state_db()) as conn:
        rows = conn.execute(
            "SELECT path FROM dlq_index WHERE created_at < ?",
            (float(before_ts),),
        ).fetchall()
        paths = [Path(str(row["path"])) for row in rows]
    deleted_files = _remove_dlq_files(paths)
    with closing(_connect_state_db()) as conn:
        deleted_paths = [str(p) for p in paths if not p.exists()]
        deleted_rows = 0
        if deleted_paths:
            placeholders = ",".join("?" * len(deleted_paths))
            cursor = conn.execute(f"DELETE FROM dlq_index WHERE path IN ({placeholders})", deleted_paths)  # noqa: S608
            conn.commit()
            deleted_rows = int(cursor.rowcount)
    return {
        "rows": deleted_rows,
        "files": deleted_files,
    }


def delete_all_dlq_entries() -> dict[str, int]:
    """Delete all DLQ index rows and their IPC payload files."""
    with closing(_connect_state_db()) as conn:
        rows = conn.execute("SELECT path FROM dlq_index").fetchall()
        paths = [Path(str(row["path"])) for row in rows]
    deleted_files = _remove_dlq_files(paths)
    with closing(_connect_state_db()) as conn:
        deleted_paths = [str(p) for p in paths if not p.exists()]
        deleted_rows = 0
        if deleted_paths:
            placeholders = ",".join("?" * len(deleted_paths))
            cursor = conn.execute(f"DELETE FROM dlq_index WHERE path IN ({placeholders})", deleted_paths)  # noqa: S608
            conn.commit()
            deleted_rows = int(cursor.rowcount)
    return {
        "rows": deleted_rows,
        "files": deleted_files,
    }


def cleanup_state_retention(
    *,
    checkpoint_retention_days: int,
    run_history_retention_days: int,
    dlq_retention_s: int,
) -> dict[str, int]:
    """Apply retention policies to checkpoint, run-history, and DLQ state."""
    now = time.time()
    checkpoint_cutoff = now - (max(0, int(checkpoint_retention_days)) * 86_400)
    run_history_cutoff = now - (max(0, int(run_history_retention_days)) * 86_400)
    dlq_cutoff = now - max(0, int(dlq_retention_s))
    dlq_result = delete_dlq_entries_before(dlq_cutoff)
    return {
        "checkpoints": delete_completed_checkpoints_before(checkpoint_cutoff),
        "runs": delete_run_history_before(run_history_cutoff),
        "dlq_rows": dlq_result["rows"],
        "dlq_files": dlq_result["files"],
    }
