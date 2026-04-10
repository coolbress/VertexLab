from __future__ import annotations

import contextlib
import logging
import os
from pathlib import Path
import shutil
import time

logger = logging.getLogger(__name__)


def _compute_cache_path() -> Path:
    app_root = os.getenv("VERTEXFORAGER_ROOT")
    if app_root:
        return Path(app_root) / "cache"
    cache_home = os.getenv("XDG_CACHE_HOME")
    return Path(cache_home) / "vertex-forager" if cache_home else Path.home() / ".cache" / "vertex-forager"


def get_app_root() -> Path:
    app_root = os.getenv("VERTEXFORAGER_ROOT")
    path = Path(app_root) if app_root else Path.home() / ".vertex_forager"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cache_dir() -> Path:
    cache_path = _compute_cache_path()
    cache_path.mkdir(parents=True, exist_ok=True)
    return cache_path


def clear_app_cache() -> None:
    vertex_root = os.getenv("VERTEXFORAGER_ROOT")
    if vertex_root and Path(vertex_root).is_symlink():
        logging.error("Safety check failed: VERTEXFORAGER_ROOT %s is a symlink", vertex_root)
        return
    cache_home = os.getenv("XDG_CACHE_HOME")
    if cache_home and Path(cache_home).is_symlink():
        logging.error("Safety check failed: XDG_CACHE_HOME %s is a symlink", cache_home)
        return
    raw_cache_dir = _compute_cache_path()
    if raw_cache_dir.is_symlink():
        logging.error("Safety check failed: Cache dir %s is a symlink, refusing to delete", raw_cache_dir)
        return

    app_root = get_app_root().resolve()
    cache_dir = raw_cache_dir.resolve()
    if app_root == Path("/").resolve() or app_root == Path.home().resolve():
        logging.error("Safety check failed: App root must not be root or home directory: %s", app_root)
        return
    expected_cache_dir = (
        (Path(vertex_root) / "cache").resolve()
        if vertex_root
        else (
            (Path(cache_home) / "vertex-forager").resolve()
            if cache_home
            else (Path.home() / ".cache" / "vertex-forager").resolve()
        )
    )

    if not cache_dir.exists():
        return
    if not cache_dir.is_dir():
        logging.error("Cache path exists but is not a directory: %s", cache_dir)
        return

    within_app_root = False
    try:
        cache_dir.relative_to(app_root)
        within_app_root = True
    except ValueError:
        within_app_root = False
    if not within_app_root and cache_dir != expected_cache_dir:
        logging.error(
            "Safety check failed: Cache dir %s is outside app root %s and not the expected cache path %s",
            cache_dir,
            app_root,
            expected_cache_dir,
        )
        return
    if cache_dir == Path("/").resolve() or cache_dir == Path.home().resolve():
        logging.error("Safety check failed: Attempting to delete root or home directory: %s", cache_dir)
        return

    shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)


def cleanup_dlq_tmp(base: Path | None, retention_s: int) -> int:
    base = base or (get_cache_dir() / "dlq")
    if not base.exists():
        return 0
    now = time.time()
    deleted = 0
    retention = float(retention_s)
    if retention < 0:
        raise ValueError("cleanup_dlq_tmp: retention_s must be non-negative")
    try:
        for f in base.rglob("*.ipc.tmp"):
            try:
                st = f.stat()
                age = now - st.st_mtime
                if age >= retention:
                    try:
                        f.unlink()
                        deleted += 1
                        with contextlib.suppress(Exception):
                            dir_fd = os.open(str(f.parent), os.O_RDONLY)
                            try:
                                os.fsync(dir_fd)
                            finally:
                                os.close(dir_fd)
                    except Exception as e_del:
                        logger.warning("DLQ cleanup failed for %s: %s", f, e_del)
            except FileNotFoundError:
                continue
    except Exception as e:
        logger.error("DLQ cleanup scan failed: %s", e)
    return deleted
