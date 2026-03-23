"""Persistence utilities for checkpointing and run history."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile

from pydantic import BaseModel, Field

from vertex_forager.core.config import RunResult
from vertex_forager.core.types import JSONValue


def get_cache_dir() -> Path:
    """Get the cache directory following XDG Base Directory spec.

    Returns:
        Path: Cache directory path (~/.cache/vertex-forager or XDG_CACHE_HOME/vertex-forager)
    """
    cache_home = os.environ.get("XDG_CACHE_HOME")
    if cache_home:
        cache_dir = Path(cache_home) / "vertex-forager"
    else:
        cache_dir = Path.home() / ".cache" / "vertex-forager"

    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def atomic_write_json(data: JSONValue, file_path: Path) -> None:
    """Atomically write JSON data to a file.

    Args:
        data: JSON-serializable data to write
        file_path: Target file path
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temporary file first
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=file_path.parent,
        prefix=f".{file_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as f:
        json.dump(data, f, indent=2, default=str)
        # Ensure data is flushed to disk
        f.flush()
        os.fsync(f.fileno())
        temp_path = Path(f.name)

    # Atomic rename
    try:
        temp_path.rename(file_path)
        # Ensure directory metadata is flushed
        dir_fd = os.open(file_path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


class Checkpoint(BaseModel):
    """Checkpoint data model for resumable runs."""

    run_id: str
    provider: str
    dataset: str
    completed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)


def save_checkpoint(checkpoint: Checkpoint) -> None:
    """Save checkpoint to disk.

    Args:
        checkpoint: Checkpoint data to save
    """
    cache_dir = get_cache_dir()
    checkpoint_dir = cache_dir / "checkpoints" / checkpoint.run_id
    checkpoint_file = checkpoint_dir / "progress.json"

    atomic_write_json(checkpoint.model_dump(), checkpoint_file)


def load_checkpoint(run_id: str) -> Checkpoint | None:
    """Load checkpoint from disk.

    Args:
        run_id: Run identifier

    Returns:
        Checkpoint if exists, None otherwise
    """
    cache_dir = get_cache_dir()
    checkpoint_file = cache_dir / "checkpoints" / run_id / "progress.json"

    if not checkpoint_file.exists():
        return None

    try:
        with open(checkpoint_file) as f:
            data = json.load(f)
        return Checkpoint(**data)
    except (json.JSONDecodeError, FileNotFoundError):
        return None


def save_run_history(run_result: RunResult, run_id: str) -> None:
    """Save run history to disk.

    Args:
        run_result: Run result to persist
        run_id: Run identifier
    """
    cache_dir = get_cache_dir()
    runs_dir = cache_dir / "runs"
    run_file = runs_dir / f"{run_id}.json"

    # Convert RunResult to serializable format
    result_data = {
        "run_id": run_id,
        "provider": run_result.provider,
        "dataset": run_result.dataset,
        "started_at": run_result.started_at,
        "finished_at": run_result.finished_at,
        "duration_s": run_result.duration_s,
        "tables": {
            table: count for table, count in run_result.tables.items()
        },
        "error_count": len(run_result.errors),
        "errors": [
            {
                "type": type(error).__name__,
                "message": str(error),
                "args": getattr(error, "args", []),
            }
            for error in run_result.errors
        ],
        "coverage_pct": run_result.coverage_pct,
    }

    atomic_write_json(result_data, run_file)
