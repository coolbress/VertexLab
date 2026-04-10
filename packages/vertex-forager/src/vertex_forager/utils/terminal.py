from __future__ import annotations

from collections.abc import Callable
import contextlib
import itertools
import logging
import shutil
import sys
import threading
import time
from typing import Any, Literal

from tqdm.auto import tqdm

from vertex_forager.core.config import ProgressSnapshot


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


def create_pbar_updater(pbar: tqdm) -> Callable[[ProgressSnapshot], None]:
    done = 0

    def _update_pbar(snapshot: ProgressSnapshot) -> None:
        nonlocal done
        delta = max(0, snapshot.jobs_done - done)
        if delta:
            pbar.update(delta)
            done += delta
        pbar.set_postfix(
            throughput=f"{snapshot.throughput_sym_per_s:.2f}/s",
            eta="n/a" if snapshot.eta_s is None else f"{snapshot.eta_s:.1f}s",
            errors=snapshot.errors,
            rows=snapshot.rows_written,
            refresh=True,
        )
        if snapshot.finished:
            pbar.close()

    return _update_pbar


class CompactLevelFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        record.levelname = original_levelname.capitalize()
        formatted = super().format(record)
        record.levelname = original_levelname
        return formatted


class ListHandler(logging.Handler):
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
        with self._message_lock:
            self.message = new_message

    def _spinner_task(self) -> None:
        while self.busy:
            spinner_char = next(self.spinner_chars)
            with self._message_lock:
                current_msg = self.message
            try:
                columns = shutil.get_terminal_size(fallback=(80, 24)).columns
            except Exception:
                columns = 80
            max_len = max(10, columns - 3)
            if len(current_msg) > max_len:
                current_msg = current_msg[: max_len - 3] + "..."
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

    def _clear_line(self) -> None:
        if not self._is_tty:
            return
        try:
            columns = shutil.get_terminal_size(fallback=(80, 24)).columns
        except Exception:
            columns = 80
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

    def __enter__(self) -> Spinner:
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
        with contextlib.suppress(Exception):
            self.root_logger.handlers = self.original_handlers
            if exc_type is not KeyboardInterrupt:
                for record in self.buffer_handler.records:
                    self.root_logger.handle(record)
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
                logging.getLogger(__name__).exception("Unexpected notebook widget update error: %s", e)
                self.busy = False
                break
            time.sleep(self.delay)
