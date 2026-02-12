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
from tqdm import tqdm

from dotenv import load_dotenv


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
    def _update_pbar(*, job: object | None = None, parse_result: object | None = None, **_kwargs: object) -> None:
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
        count = len(symbol.split(","))
        
        # Check for pagination
        # If next_jobs exist, this logical request is not yet complete.
        next_jobs = getattr(parse_result, "next_jobs", None) if parse_result else None
        
        if next_jobs:
            # Paging in progress: Update status only
            pbar.set_postfix_str(f"Paging: {symbol[:20]}..", refresh=True)
        else:
            # Batch Complete (Fully Processed): Update progress count
            pbar.update(count)
            pbar.set_postfix_str(f"Done: {symbol[:20]}..", refresh=True)
    
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
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class Spinner:
    """
    Spinner class using sys.stderr for direct terminal output.
    
    Uses standard ANSI escape codes for line clearing and updates.
    Runs a background thread to animate the spinner character.
    """

    def __init__(self, message: str = "Processing...", delay: float = 0.1):
        self.message = message
        self.delay = delay
        self.busy = False
        self.update_thread = None
        self._message_lock = threading.Lock()
        
        # Spinner chars
        self.spinner_chars = itertools.cycle(['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'])

    def update_message(self, new_message: str):
        """Update the message displayed next to the spinner."""
        with self._message_lock:
            self.message = new_message

    def _spinner_task(self):
        """Background task to animate the spinner."""
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
                current_msg = current_msg[:max_len-3] + "..."
            
            # Write to stderr: \r to return to start, \033[K to clear line from cursor
            sys.stderr.write(f"\r{spinner_char} {current_msg}\033[K")
            sys.stderr.flush()
            
            time.sleep(self.delay)

    def start(self):
        """Start the spinner."""
        self._hide_cursor()
        self.busy = True
        self.update_thread = threading.Thread(target=self._spinner_task, daemon=True)
        self.update_thread.start()

    def stop(self, clear: bool = True):
        """Stop the spinner and cleanup.
        
        Args:
            clear: Whether to clear the spinner line.
        """
        self.busy = False
        if self.update_thread:
            try:
                self.update_thread.join(timeout=0.2)
            except Exception:
                pass
        
        if clear:
            self._clear_line()
        else:
            sys.stderr.write("\n")
            sys.stderr.flush()
            
        self._show_cursor()

    def _clear_line(self):
        """Clear the current line in stderr."""
        try:
            columns = shutil.get_terminal_size(fallback=(80, 24)).columns
        except Exception:
            columns = 80
            
        # Overwrite with spaces to ensure clearing in Jupyter/dumb terminals
        sys.stderr.write(f"\r{' ' * (columns - 1)}\r")
        sys.stderr.flush()

    def _hide_cursor(self):
        """Hide the cursor."""
        sys.stderr.write("\033[?25l")
        sys.stderr.flush()

    def _show_cursor(self):
        """Show the cursor."""
        sys.stderr.write("\033[?25h")
        sys.stderr.flush()

    def __enter__(self):
        # Setup logging capture like before
        self.root_logger = logging.getLogger()
        self.original_handlers = self.root_logger.handlers[:]
        self.buffer_handler = ListHandler()
        self.root_logger.handlers = [self.buffer_handler]

        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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

def clear_app_cache():
    """캐시 디렉토리 내부를 완전히 비웁니다."""
    cache_dir = get_cache_dir()
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        cache_dir.mkdir()


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
            # Ensure the background task is cancelled when user interrupts
            task.cancel()
            try:
                # Wait for cancellation to complete
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
            raise

    return wrapper
