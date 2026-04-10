from __future__ import annotations

import contextlib
import logging
import os
from pathlib import Path
import shutil
import tempfile
import time

logger = logging.getLogger(__name__)
APP_ROOT_MARKER = ".vertex-forager-root"
CACHE_MARKER = ".vertex-forager-cache"
SYSTEM_PATHS = tuple(path.resolve() for path in (Path("/var"), Path("/usr"), Path("/etc"), Path("/opt")))
TEMP_PATH = Path(tempfile.gettempdir()).resolve()


def _compute_cache_path() -> Path:
    app_root = os.getenv("VERTEXFORAGER_ROOT")
    if app_root:
        return Path(app_root) / "cache"
    cache_home = os.getenv("XDG_CACHE_HOME")
    return Path(cache_home) / "vertex-forager" if cache_home else Path.home() / ".cache" / "vertex-forager"


def _compute_app_root() -> Path:
    app_root = os.getenv("VERTEXFORAGER_ROOT")
    return Path(app_root) if app_root else Path.home() / ".vertex_forager"


def _touch_marker(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def _is_under(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def get_app_root() -> Path:
    path = _compute_app_root()
    path.mkdir(parents=True, exist_ok=True)
    _touch_marker(path / APP_ROOT_MARKER)
    return path


def get_cache_dir() -> Path:
    cache_path = _compute_cache_path()
    cache_path.mkdir(parents=True, exist_ok=True)
    _touch_marker(cache_path / CACHE_MARKER)
    return cache_path


def clear_app_cache() -> bool:
    vertex_root = os.getenv("VERTEXFORAGER_ROOT")
    if vertex_root and Path(vertex_root).is_symlink():
        logging.error("Safety check failed: VERTEXFORAGER_ROOT %s is a symlink", vertex_root)
        return False
    cache_home = os.getenv("XDG_CACHE_HOME")
    if cache_home and Path(cache_home).is_symlink():
        logging.error("Safety check failed: XDG_CACHE_HOME %s is a symlink", cache_home)
        return False
    raw_cache_dir = _compute_cache_path()
    if raw_cache_dir.is_symlink():
        logging.error("Safety check failed: Cache dir %s is a symlink, refusing to delete", raw_cache_dir)
        return False

    app_root = _compute_app_root().resolve()
    cache_dir = raw_cache_dir.resolve()
    if app_root == Path("/").resolve() or app_root == Path.home().resolve():
        logging.error("Safety check failed: App root must not be root or home directory: %s", app_root)
        return False
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
        logger.info("Cache dir %s does not exist; nothing to clear", cache_dir)
        return True
    if not cache_dir.is_dir():
        logging.error("Cache path exists but is not a directory: %s", cache_dir)
        return False
    if any(
        (cache_dir == system_path or _is_under(cache_dir, system_path)) and not _is_under(cache_dir, TEMP_PATH)
        for system_path in SYSTEM_PATHS
    ):
        logging.error("Safety check failed: Refusing to delete system path: %s", cache_dir)
        return False
    if hasattr(os, "geteuid") and cache_dir.stat().st_uid != os.geteuid():
        logging.error("Safety check failed: Cache dir %s is not owned by the current user", cache_dir)
        return False
    if not os.access(cache_dir, os.W_OK):
        logging.error("Safety check failed: Cache dir %s is not writable", cache_dir)
        return False
    within_app_root = _is_under(cache_dir, app_root)
    has_marker = (cache_dir / CACHE_MARKER).exists() or (app_root / APP_ROOT_MARKER).exists()
    if cache_dir != expected_cache_dir and not (within_app_root and has_marker):
        logging.error(
            "Safety check failed: Cache dir %s is outside app root %s or missing marker; expected cache path is %s",
            cache_dir,
            app_root,
            expected_cache_dir,
        )
        return False
    if cache_dir == Path("/").resolve() or cache_dir == Path.home().resolve():
        logging.error("Safety check failed: Attempting to delete root or home directory: %s", cache_dir)
        return False

    shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    _touch_marker(cache_dir / CACHE_MARKER)
    return True


def cleanup_dlq_tmp(base: Path | None, retention_s: int) -> int:
    retention = float(retention_s)
    if retention < 0:
        raise ValueError("cleanup_dlq_tmp: retention_s must be non-negative")
    base = base or (get_cache_dir() / "dlq")
    if not base.exists():
        return 0
    now = time.time()
    deleted = 0
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
