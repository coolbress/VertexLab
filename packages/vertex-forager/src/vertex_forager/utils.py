import asyncio
from collections.abc import Callable
import contextlib
import functools
import itertools
import logging
import math
import os
from pathlib import Path
import re
import shutil
import sys
import threading
import time
from typing import Any, Literal, ParamSpec
import warnings

from dotenv import load_dotenv
import nest_asyncio
import psutil
from tqdm.auto import tqdm

from vertex_forager.exceptions import InputError

logger = logging.getLogger(__name__)


def _safe_get_ipython() -> Any:
    try:
        import importlib

        mod = importlib.import_module("IPython")
        func = getattr(mod, "get_ipython", None)
        if callable(func):
            return func()
        return None
    except Exception:
        return None


def _ipython_display(value: Any) -> None:
    import importlib

    with contextlib.suppress(Exception):
        mod = importlib.import_module("IPython.display")
        func = getattr(mod, "display", None)
        if callable(func):
            func(value)


def as_dict(obj: Any) -> dict[str, Any]:
    """Convert a pipeline result object to a serializable dictionary.

    Handles cases where the object is None or has different metric structures.
    """
    if obj is None:
        return {}
    return {
        "counters": getattr(obj, "metrics_counters", {}),
        "histograms": getattr(obj, "metrics_histograms", {}),
        "summary": getattr(obj, "metrics_summary", {}),
        "tables": getattr(obj, "tables", {}),
        "errors": getattr(obj, "errors", []),
    }


def set_env(cfg: dict[str, Any]) -> None:
    """Apply a configuration dictionary to environment variables.

    If a value is None, the environment variable is removed.
    Otherwise, it is set to the string representation of the value.
    """
    for k, v in cfg.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = str(v)


def load_tickers_env(name: str, default: list[str]) -> list[str]:
    """Load a list of tickers from an environment variable.

    Args:
        name: Environment variable name.
        default: Default list to return if variable is missing or empty.

    Returns:
        List of uppercase, stripped ticker symbols.
    """
    v = os.getenv(name)
    if not v:
        return list(default)
    toks = [t.strip().upper() for t in v.split(",") if t.strip()]
    return toks if toks else list(default)


def process_symbols(tickers: list[str] | None) -> list[str] | None:
    """Convert tickers to normalized symbols.

    Args:
        tickers: List of ticker symbols (may contain whitespace).

    Returns:
        List of normalized symbols (uppercase, stripped), or None if tickers is None.
    """
    if tickers is not None:
        return [t.strip().upper() for t in tickers if t and t.strip()]
    return None


def validate_tickers(symbols: list[str] | tuple[str, ...]) -> None:
    """Validate a list of ticker symbols.

    Args:
        symbols: Ticker symbols container (list or tuple of strings).

    Raises:
        InputError: If symbols is not a list/tuple, empty, contains non-string items,
                    or any item is empty or has leading/trailing whitespace.
    """
    if not isinstance(symbols, (list, tuple)):
        raise InputError("tickers must be a list or tuple of strings")
    if len(symbols) == 0:
        raise InputError("tickers list cannot be empty")
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise InputError("each ticker must be a string")
        stripped = symbol.strip()
        if not stripped or stripped != symbol:
            raise InputError("tickers must be non-empty and must not include leading/trailing whitespace")


def env_bool(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable.

    Args:
        name: Environment variable name.
        default: Default value if not set.

    Returns:
        True if value is "1", "true", "yes", "on" (case-insensitive).
    """
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    truthy = ("1", "true", "yes", "on")
    falsy = ("0", "false", "no", "off")
    if s in truthy:
        return True
    if s in falsy:
        return False
    return default


def env_int(name: str, default: int | None = None) -> int | None:
    """Read an integer environment variable.

    Args:
        name: Environment variable name.
        default: Default value if not set or invalid.

    Returns:
        Integer value, or default if parsing fails.
    """
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float | None = None) -> float | None:
    """Read a float environment variable.

    Args:
        name: Environment variable name.
        default: Default value if not set or invalid.

    Returns:
        Float value, or default if parsing fails.
    """
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v.strip())
    except (TypeError, ValueError):
        return default


def validate_memory_usage(
    symbols: list[str] | None,
    connect_db: str | Path | None,
    bytes_per_item: int = 1 * 1024 * 1024,
) -> None:
    """Validate memory safety for per-ticker jobs.

    Args:
        symbols: List of ticker symbols for the per-ticker job.
        connect_db: Database connection path or None for in-memory.
        bytes_per_item: Estimated bytes per ticker for the dataset.
    """
    if connect_db is not None:
        return
    if symbols is None:
        return
    if not isinstance(bytes_per_item, int) or bytes_per_item <= 0:
        raise ValueError("bytes_per_item must be a positive integer")
    num_items = len(symbols)
    estimated_size = num_items * bytes_per_item
    available_memory = psutil.virtual_memory().available
    check_memory_safety(estimated_size, available_memory, num_items)


def check_memory_safety(
    estimated_size: int,
    available_memory: int,
    num_tickers: int,
    threshold_ratio: float = 0.7,
    threshold_absolute: int = 4 * 1024 * 1024 * 1024,
) -> None:
    """Check if the request is safe for memory usage.

    Args:
        estimated_size: Estimated size of the data in bytes.
        available_memory: Available system memory in bytes.
        num_tickers: Number of tickers/items being requested (for log message).
        threshold_ratio: Ratio of available memory to trigger warning.
        threshold_absolute: Absolute size in bytes to trigger warning.
    """
    with contextlib.suppress(TypeError, ValueError):
        env_ratio = float(os.getenv("VF_MEM_THRESHOLD_RATIO", "").strip() or threshold_ratio)
        if 0 < env_ratio <= 1:
            threshold_ratio = env_ratio
        else:
            logger.debug(f"Ignoring invalid VF_MEM_THRESHOLD_RATIO={env_ratio} (must be 0 < x <= 1)")
    with contextlib.suppress(TypeError, ValueError, OverflowError):
        _abs_str = os.getenv("VF_MEM_THRESHOLD_ABS_MB", "").strip()
        env_abs_mb = float(_abs_str or (threshold_absolute / 1024 / 1024))
        if env_abs_mb > 0 and math.isfinite(env_abs_mb):
            threshold_absolute = int(env_abs_mb * 1024 * 1024)
        else:
            logger.debug(f"Ignoring invalid VF_MEM_THRESHOLD_ABS_MB={_abs_str}")
    if estimated_size > available_memory * threshold_ratio:
        warnings.warn(
            f"High memory usage warning: Requesting data for {num_tickers} symbols "
            f"(est. {estimated_size / 1024 / 1024:.0f} MB) with only "
            f"{available_memory / 1024 / 1024:.0f} MB available. "
            "Consider using 'connect_db' to save to disk.",
            UserWarning,
            stacklevel=3,
        )
    elif estimated_size > threshold_absolute:
        warnings.warn(
            f"Large data request warning: Requesting data for {num_tickers} symbols "
            f"(est. {estimated_size / 1024 / 1024 / 1024:.1f} GB). "
            "This may impact system performance. "
            "Consider using 'connect_db' to save to disk.",
            UserWarning,
            stacklevel=3,
        )


P = ParamSpec("P")


def create_pbar_updater(pbar: tqdm) -> Callable[P, None]:
    """Create a progress bar update callback.

    Args:
        pbar: The tqdm progress bar instance.

    Returns:
        Callable to update the progress bar.
    """

    def _update_pbar(*args: P.args, **kwargs: P.kwargs) -> None:
        """Update progress bar based on fully processed ticker count.

        With Smart Batching, we expect most requests to complete in a single fetch.
        However, if pagination occurs (e.g. 10k row limit hit), we only update the
        counter when the FINAL page is processed to avoid double counting.
        """
        job = kwargs.get("job")
        parse_result = kwargs.get("parse_result")
        if not job:
            return

        symbol = getattr(job, "symbol", "")
        if not isinstance(symbol, str) or not symbol:
            return

        # Calculate number of symbols in this batch
        # Filter out empty strings from split result (e.g. "AAPL,," -> ["AAPL"])
        tokens = [s.strip() for s in symbol.split(",") if s.strip()]
        count = len(tokens)

        # Use a safe display value (first token)
        display_symbol = tokens[0] if tokens else "Unknown"
        if len(tokens) > 1:
            display_symbol += f" (+{len(tokens) - 1})"

        # Check for pagination
        # If next_jobs exist, this logical request is not yet complete.
        next_jobs = getattr(parse_result, "next_jobs", None) if parse_result else None

        if next_jobs:
            pbar.set_postfix_str("Pagination processing", refresh=True)
        else:
            pbar.update(count)
            pbar.set_postfix_str(f"Done: {display_symbol}..", refresh=True)

    return _update_pbar


class CompactLevelFormatter(logging.Formatter):
    """Formatter that renders the log level as a compact, capitalized label."""

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        record.levelname = original_levelname.capitalize()
        formatted = super().format(record)
        record.levelname = original_levelname
        return formatted


class ListHandler(logging.Handler):
    """Handler that stores log records in memory for later inspection."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class Spinner:
    def __init__(
        self,
        message: str = "Processing...",
        delay: float = 0.1,
        persist: bool = False,
    ) -> None:
        self.message = message
        self.delay = delay
        self.persist = persist
        self.busy = False
        self.update_thread: threading.Thread | None = None
        self._message_lock = threading.Lock()

        self._is_tty = sys.stderr.isatty()
        ip = _safe_get_ipython()
        self._is_notebook = bool(ip and ip.__class__.__name__ == "ZMQInteractiveShell")
        self._widget_label: Any | None = None

        self.spinner_chars = itertools.cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])

    def update_message(self, new_message: str) -> None:
        """Update the message displayed next to the spinner."""
        with self._message_lock:
            self.message = new_message

    def _spinner_task(self) -> None:
        while self.busy:
            spinner_char = next(self.spinner_chars)

            with self._message_lock:
                current_msg = self.message

            # Get terminal width
            try:
                columns = shutil.get_terminal_size(fallback=(80, 24)).columns
            except Exception:
                columns = 80

            # Truncate message to fit one line
            # -2 for spinner char + space, -1 for cursor safety margin
            max_len = max(10, columns - 3)
            if len(current_msg) > max_len:
                current_msg = current_msg[: max_len - 3] + "..."

            # Write to stderr: \r to return to start, \033[K to clear line from cursor
            sys.stderr.write(f"\r{spinner_char} {current_msg}\033[K")
            sys.stderr.flush()

            time.sleep(self.delay)

    def start(self) -> None:
        self.busy = True

        if self._is_notebook:
            try:
                from ipywidgets import HTML

                self._widget_label = HTML(value=f"⏳ {self.message}")
                _ipython_display(self._widget_label)
                t = threading.Thread(target=self._notebook_task, daemon=True)
                self.update_thread = t
                t.start()
            except ImportError:
                self._is_notebook = False
                if self._is_tty:
                    self._hide_cursor()
                    t = threading.Thread(target=self._spinner_task, daemon=True)
                    self.update_thread = t
                    t.start()
                else:
                    sys.stderr.write(f"{self.message}\n")
                    sys.stderr.flush()
        elif self._is_tty:
            self._hide_cursor()
            t = threading.Thread(target=self._spinner_task, daemon=True)
            self.update_thread = t
            t.start()
        else:
            sys.stderr.write(f"{self.message}\n")
            sys.stderr.flush()

    def stop(self, clear: bool = True) -> None:
        self.busy = False

        if self._is_notebook:
            if self.update_thread:
                timeout = min(max(self.delay + 0.1, 0.1), 2.0)
                with contextlib.suppress(Exception):
                    self.update_thread.join(timeout=timeout)
            if self._widget_label:
                if self.persist:
                    self._widget_label.value = f"✅ {self.message}"
                elif clear:
                    with contextlib.suppress(Exception):
                        self._widget_label.layout.display = "none"
        elif self._is_tty:
            if self.update_thread:
                timeout = min(max(self.delay + 0.1, 0.1), 2.0)
                with contextlib.suppress(Exception):
                    self.update_thread.join(timeout=timeout)
            if self.persist:
                self._clear_line()
                sys.stderr.write(f"✅ {self.message}\n")
                sys.stderr.flush()
            else:
                if clear:
                    self._clear_line()
                else:
                    sys.stderr.write("\n")
                    sys.stderr.flush()
            self._show_cursor()
        else:
            ...

    def _clear_line(self) -> None:
        if not self._is_tty:
            return

        try:
            columns = shutil.get_terminal_size(fallback=(80, 24)).columns
        except Exception:
            columns = 80

        # Overwrite with spaces to ensure clearing in Jupyter/dumb terminals
        sys.stderr.write(f"\r{' ' * (columns - 1)}\r")
        sys.stderr.flush()

    def _hide_cursor(self) -> None:
        if self._is_tty:
            sys.stderr.write("\033[?25l")
            sys.stderr.flush()

    def _show_cursor(self) -> None:
        if self._is_tty:
            sys.stderr.write("\033[?25h")
            sys.stderr.flush()

    def __enter__(self) -> "Spinner":
        self.root_logger = logging.getLogger()
        self.original_handlers = self.root_logger.handlers[:]
        self.buffer_handler = ListHandler()
        self.root_logger.handlers = [self.buffer_handler]

        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> Literal[False]:
        if exc_type is KeyboardInterrupt:
            self.stop(clear=False)
            sys.stderr.write("Aborted by user.\n")
            sys.stderr.flush()
        else:
            self.stop(clear=True)

        # Restore logging handlers
        with contextlib.suppress(Exception):
            self.root_logger.handlers = self.original_handlers
            if exc_type is not KeyboardInterrupt:
                for record in self.buffer_handler.records:
                    self.root_logger.handle(record)

        if exc_type is KeyboardInterrupt:
            return False
        return False

    def _notebook_task(self) -> None:
        failures = 0
        while self.busy and self._widget_label is not None:
            ch = next(self.spinner_chars)
            with self._message_lock:
                msg = self.message
            try:
                self._widget_label.value = f"{ch} {msg}"
                failures = 0
            except (AttributeError, RuntimeError) as e:
                logging.getLogger(__name__).error("%s", e)
                failures += 1
                if failures >= 3:
                    self.busy = False
                    break
            except Exception as e:
                logging.getLogger(__name__).exception(
                    "Unexpected notebook widget update error: %s",
                    e,
                )
                self.busy = False
                break
            time.sleep(self.delay)


def get_app_root() -> Path:
    """Return the application data root directory.

    Uses the 'VERTEXFORAGER_ROOT' environment variable when set; otherwise falls
    back to the '.vertex_forager' directory under the user's home. The directory
    is created if it does not exist.

    Returns:
        Path: Absolute path to the data root directory. If the environment
        variable is unset, this is '$HOME/.vertex_forager'.

    Raises:
        OSError: If creating the directory fails due to permission or filesystem errors.
    """
    app_root = os.getenv("VERTEXFORAGER_ROOT")
    path = Path(app_root) if app_root else Path.home() / ".vertex_forager"
    path.mkdir(parents=True, exist_ok=True)
    return path


def sanitize_field(v: object) -> str:
    """Normalize arbitrary input to a safe, single-token string.

    Purpose:
        Converts any object into a sanitized string suitable for key=value logs,
        filenames, or identifiers where whitespace and delimiters can break parsing.

    Args:
        v (object): Input value to normalize. May be None or any object with a string
            representation.

    Returns:
        str: Normalized string with these rules:
            - None maps to an empty string ("").
            - All whitespace sequences are replaced with a single underscore ("_").
            - The "=" character is replaced with "_".
            - Multiple consecutive underscores are collapsed to one.
            - Leading and trailing underscores are trimmed.

    Examples:
        - sanitize_field(None) -> ""
        - sanitize_field("  a  b\tc  ") -> "a_b_c"
        - sanitize_field("x==y") -> "x_y"
        - sanitize_field("  __  ") -> ""
    """
    s = "" if v is None else str(v)
    s = re.sub(r"\s+", "_", s)
    s = s.replace("=", "_")
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s


def get_cache_dir() -> Path:
    """Return the temporary cache directory under the app root.

    Returns:
        Path: Absolute path to the cache directory. Created if missing.
    """
    cache_path = get_app_root() / "cache"
    cache_path.mkdir(exist_ok=True)
    return cache_path


def clear_app_cache() -> None:
    """Clear all contents of the application cache directory.

    Returns:
        None

    Raises:
        OSError: If deletion or re-creation of the cache directory fails.
    """
    app_root = get_app_root().resolve()
    cache_dir = get_cache_dir().resolve()

    # 1. Check existence and type
    if not cache_dir.exists():
        return
    if not cache_dir.is_dir():
        logging.error(f"Cache path exists but is not a directory: {cache_dir}")
        return

    # 2. Safety check: ensure cache_dir is a descendant of app_root
    try:
        cache_dir.relative_to(app_root)
    except ValueError:
        logging.error(f"Safety check failed: Cache dir {cache_dir} is not inside app root {app_root}")
        return

    # 3. Safety check: prevent deleting root or home
    if cache_dir == Path("/").resolve() or cache_dir == Path.home().resolve():
        logging.error(f"Safety check failed: Attempting to delete root or home directory: {cache_dir}")
        return

    # Safe to delete
    shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)


def cleanup_dlq_tmp(base: Path | None, retention_s: int) -> int:
    """Remove stale DLQ temporary files (*.ipc.tmp) under the given DLQ root.

    Args:
        base: Base DLQ directory. If None, defaults to get_cache_dir()/\"dlq\".
        retention_s: Age threshold in seconds; files older than this are deleted.

    Returns:
        int: Number of files deleted.

    Raises:
        ValueError: If retention_s is negative.
    """
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


def load_env_file(env_file: Path | None = None) -> None:
    """Load environment variables from a .env file.

    Args:
        env_file: Path to .env file. Defaults to project-root '.env' lookup by dotenv.
    """
    load_dotenv(dotenv_path=env_file, override=False)


def jupyter_safe(async_func: Callable[..., Any]) -> Callable[..., Any]:
    """Run an async function in both scripts and Jupyter environments.

    Args:
        async_func: Async callable to wrap.

    Returns:
        A callable that returns the awaited result.
    """

    @functools.wraps(async_func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(async_func(*args, **kwargs))

        nest_asyncio.apply()
        task = loop.create_task(async_func(*args, **kwargs))
        try:
            return loop.run_until_complete(task)
        except KeyboardInterrupt:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)
            raise

    return wrapper
