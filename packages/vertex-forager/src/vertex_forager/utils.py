import asyncio
import functools
import os
import shutil
from pathlib import Path
import logging
import threading
import time
import itertools
import sys
import warnings
from typing import Any, Callable

import nest_asyncio
from tqdm.auto import tqdm
import psutil

from dotenv import load_dotenv
from vertex_forager.exceptions import InputError
logger = logging.getLogger(__name__)


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
    threshold_absolute: int = 4 * 1024 * 1024 * 1024,  # 4GB
) -> None:
    """Check if the request is safe for memory usage.

    Args:
        estimated_size: Estimated size of the data in bytes.
        available_memory: Available system memory in bytes.
        num_tickers: Number of tickers/items being requested (for log message).
        threshold_ratio: Ratio of available memory to trigger warning.
        threshold_absolute: Absolute size in bytes to trigger warning.
    """
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


def create_pbar_updater(pbar: tqdm) -> Callable:
    """Create a progress bar update callback.

    Args:
        pbar: The tqdm progress bar instance.

    Returns:
        Callable to update the progress bar.
    """

    def _update_pbar(
        *,
        job: object | None = None,
        parse_result: object | None = None,
        **_kwargs: object,
    ) -> None:
        """Update progress bar based on fully processed ticker count.

        With Smart Batching, we expect most requests to complete in a single fetch.
        However, if pagination occurs (e.g. 10k row limit hit), we only update the
        counter when the FINAL page is processed to avoid double counting.
        """
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
    """로그 레벨을 'Info:' 형태로 표기하는 Formatter."""

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        record.levelname = original_levelname.capitalize()
        formatted = super().format(record)
        record.levelname = original_levelname
        return formatted


class ListHandler(logging.Handler):
    """로그 레코드를 메모리에 저장하는 핸들러."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class Spinner:
    def __init__(self, message: str = "Processing...", delay: float = 0.1, persist: bool = False) -> None:
        self.message = message
        self.delay = delay
        self.persist = persist
        self.busy = False
        self.update_thread = None
        self._message_lock = threading.Lock()

        self._is_tty = sys.stderr.isatty()
        ip = None
        try:
            from IPython import get_ipython
            ip = get_ipython()
        except (ImportError, ModuleNotFoundError, AttributeError):
            ip = None
        except Exception as e:
            logger.error("Unexpected error during notebook detection: %s", e)
            ip = None
        self._is_notebook = bool(ip and ip.__class__.__name__ == "ZMQInteractiveShell")
        self._widget_label: object | None = None

        self.spinner_chars = itertools.cycle(
            ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        )

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
                from ipywidgets import HTML  # type: ignore
                from IPython.display import display  # type: ignore
                self._widget_label = HTML(value=f"⏳ {self.message}")
                display(self._widget_label)
                self.update_thread = threading.Thread(
                    target=self._notebook_task, daemon=True
                )
                self.update_thread.start()
            except ImportError:
                self._is_notebook = False
                if self._is_tty:
                    self._hide_cursor()
                    self.update_thread = threading.Thread(
                        target=self._spinner_task, daemon=True
                    )
                    self.update_thread.start()
                else:
                    sys.stderr.write(f"{self.message}\n")
                    sys.stderr.flush()
        elif self._is_tty:
            self._hide_cursor()
            self.update_thread = threading.Thread(
                target=self._spinner_task, daemon=True
            )
            self.update_thread.start()
        else:
            sys.stderr.write(f"{self.message}\n")
            sys.stderr.flush()

    def stop(self, clear: bool = True) -> None:
        self.busy = False

        if self._is_notebook:
            if self.update_thread:
                try:
                    timeout = min(max(self.delay + 0.1, 0.1), 2.0)
                    self.update_thread.join(timeout=timeout)
                except Exception:
                    pass
            if self._widget_label:
                if self.persist:
                    self._widget_label.value = f"✅ {self.message}"
                elif clear:
                    try:
                        self._widget_label.layout.display = "none"
                    except Exception:
                        pass
        elif self._is_tty:
            if self.update_thread:
                try:
                    timeout = min(max(self.delay + 0.1, 0.1), 2.0)
                    self.update_thread.join(timeout=timeout)
                except Exception:
                    pass
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
    ) -> bool:
        if exc_type is KeyboardInterrupt:
            self.stop(clear=False)
            sys.stderr.write("Aborted by user.\n")
            sys.stderr.flush()
        else:
            self.stop(clear=True)

        # Restore logging handlers
        try:
            self.root_logger.handlers = self.original_handlers
            # Do not replay logs on interrupt to keep output clean
            if exc_type is not KeyboardInterrupt:
                for record in self.buffer_handler.records:
                    self.root_logger.handle(record)
        except Exception:
            pass

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
                # type: ignore[attr-defined]
                self._widget_label.value = f"{ch} {msg}"
                failures = 0
            except (AttributeError, RuntimeError) as e:
                logging.getLogger(__name__).error("%s", e)
                failures += 1
                if failures >= 3:
                    self.busy = False
                    break
            except Exception as e:
                logging.getLogger(__name__).exception("Unexpected notebook widget update error: %s", e)
                self.busy = False
                break
            time.sleep(self.delay)


def get_app_root() -> Path:
    """
    데이터 저장소 루트 경로를 반환합니다.
    환경 변수 'VERTEXFORAGER_ROOT'가 없으면 홈 디렉토리의 '.vertex_forager'를 사용합니다.
    """
    app_root = os.getenv("VERTEXFORAGER_ROOT")
    path = Path(app_root) if app_root else Path.home() / ".vertex_forager"

    # 별도의 init 명령 없이도 실행 시점에 폴더가 없으면 생성 (Lazy Initialization)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cache_dir() -> Path:
    """임시 캐시 디렉토리를 반환합니다."""
    cache_path = get_app_root() / "cache"
    cache_path.mkdir(exist_ok=True)
    return cache_path


def clear_app_cache() -> None:
    """캐시 디렉토리 내부를 완전히 비웁니다."""
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
        logging.error(
            f"Safety check failed: Cache dir {cache_dir} is not inside app root {app_root}"
        )
        return

    # 3. Safety check: prevent deleting root or home
    if cache_dir == Path("/").resolve() or cache_dir == Path.home().resolve():
        logging.error(
            f"Safety check failed: Attempting to delete root or home directory: {cache_dir}"
        )
        return

    # Safe to delete
    shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)


def load_env_file(env_file: Path | None = None) -> None:
    """Load environment variables from a .env file.

    Args:
        env_file: Path to .env file. Defaults to project-root '.env' lookup by dotenv.
    """
    load_dotenv(dotenv_path=env_file, override=False)

def mask_secret(value: str, keep: int = 4) -> str:
    """Mask sensitive strings for safe logging.

    Args:
        value: The secret value to mask.
        keep: Number of trailing characters to keep unmasked (default: 4).

    Returns:
        A masked string where leading characters are replaced with asterisks.

    Notes:
        - If `keep` <= 0, the entire string is masked.
        - If the secret length is less than or equal to `keep`, the entire string is masked.
        - Whitespace is trimmed before masking.
    """
    if not isinstance(value, str):
        return "***"
    n = max(0, keep)
    s = value.strip()
    if n == 0 or len(s) <= n:
        return "*" * len(s)
    return "*" * (len(s) - n) + s[-n:]


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
            # Ensure the background task is cancelled when user interrupts
            if not task.done():
                task.cancel()
                try:
                    # Wait for cancellation to complete
                    loop.run_until_complete(task)
                except asyncio.CancelledError:
                    pass
            raise

    return wrapper
