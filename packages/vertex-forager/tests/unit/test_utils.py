import os
from pathlib import Path
import time
import types
from unittest.mock import MagicMock, patch
import warnings

import pytest
from tqdm import tqdm

from vertex_forager.core.config import ProgressSnapshot
from vertex_forager.core.errors import RunError
from vertex_forager.core.http import _redact_urls
from vertex_forager.exceptions import InputError
from vertex_forager.utils import (  # type: ignore[attr-defined]
    CompactLevelFormatter,
    ListHandler,
    _ipython_display,
    _safe_get_ipython,
    as_dict,
    check_memory_safety,
    cleanup_dlq_tmp,
    clear_app_cache,
    create_pbar_updater,
    env_bool,
    env_float,
    env_int,
    get_app_root,
    get_cache_dir,
    jupyter_safe,
    load_env_file,
    load_tickers_env,
    process_symbols,
    sanitize_field,
    set_env,
    validate_memory_usage,
    validate_tickers,
)


class TestPbarUpdater:
    def test_update_pbar_counts_symbols_correctly(self):
        """update_pbar reflects jobs_done deltas from ProgressSnapshot."""
        mock_pbar = MagicMock(spec=tqdm)
        updater = create_pbar_updater(mock_pbar)

        updater(
            ProgressSnapshot(
                jobs_done=3,
                jobs_total=5,
                pct=60.0,
                throughput_sym_per_s=2.5,
                eta_s=1.0,
                errors=0,
                retries=0,
                rows_written=10,
                elapsed_s=1.2,
                active_workers=1,
                pending_jobs=2,
                throttle_events=0,
                dlq_spooled=0,
                memory_mb=100.0,
                cpu_pct=5.0,
            )
        )
        mock_pbar.update.assert_called_with(3)
        mock_pbar.set_postfix.assert_called()

        mock_pbar.reset_mock()
        updater(
            ProgressSnapshot(
                jobs_done=5,
                jobs_total=5,
                pct=100.0,
                throughput_sym_per_s=3.0,
                eta_s=0.0,
                errors=1,
                retries=2,
                rows_written=20,
                elapsed_s=2.0,
                active_workers=0,
                pending_jobs=0,
                throttle_events=1,
                dlq_spooled=0,
                memory_mb=101.0,
                cpu_pct=6.0,
            )
        )
        mock_pbar.update.assert_called_with(2)

    def test_update_pbar_handles_pagination(self):
        """No delta means no bar advance, but postfix still updates."""
        mock_pbar = MagicMock(spec=tqdm)
        updater = create_pbar_updater(mock_pbar)
        updater(
            ProgressSnapshot(
                jobs_done=1,
                jobs_total=5,
                pct=20.0,
                throughput_sym_per_s=1.0,
                eta_s=4.0,
                errors=0,
                retries=0,
                rows_written=5,
                elapsed_s=1.0,
                active_workers=1,
                pending_jobs=3,
                throttle_events=0,
                dlq_spooled=0,
                memory_mb=100.0,
                cpu_pct=5.0,
            )
        )
        mock_pbar.reset_mock()
        updater(
            ProgressSnapshot(
                jobs_done=1,
                jobs_total=5,
                pct=20.0,
                throughput_sym_per_s=1.5,
                eta_s=3.0,
                errors=0,
                retries=0,
                rows_written=8,
                elapsed_s=1.5,
                active_workers=1,
                pending_jobs=2,
                throttle_events=0,
                dlq_spooled=0,
                memory_mb=101.0,
                cpu_pct=6.0,
            )
        )
        mock_pbar.update.assert_not_called()
        mock_pbar.set_postfix.assert_called()


class TestCacheUtils:
    @patch("vertex_forager.utils.get_app_root")
    @patch("vertex_forager.utils.get_cache_dir")
    @patch("shutil.rmtree")
    def test_clear_app_cache_safety_check_pass(self, mock_rmtree, mock_get_cache, mock_get_root, tmp_path: Path):
        """Test that clear_app_cache proceeds when cache is inside app root."""
        # Setup paths
        # Use resolve() to handle symlinks (e.g. /tmp -> /private/tmp on macOS)
        root_path = (tmp_path / "vertex_root").resolve()
        cache_path = (root_path / "cache").resolve()

        mock_get_root.return_value = root_path
        mock_get_cache.return_value = cache_path

        # Mock existence and is_dir
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "is_dir", return_value=True),
            patch.object(Path, "mkdir") as mock_mkdir,
        ):
            clear_app_cache()

            # Should verify relative_to and call rmtree
            mock_rmtree.assert_called_once_with(cache_path)
            mock_mkdir.assert_called_once()

    @patch("vertex_forager.utils.get_app_root")
    @patch("vertex_forager.utils.get_cache_dir")
    @patch("shutil.rmtree")
    def test_clear_app_cache_safety_check_fail(self, mock_rmtree, mock_get_cache, mock_get_root, tmp_path: Path):
        """Test that clear_app_cache aborts when cache is outside app root."""
        # Setup paths that are disjoint
        root_path = (tmp_path / "vertex_root").resolve()
        cache_path = Path("/etc/passwd").resolve()  # Dangerous path!

        mock_get_root.return_value = root_path
        mock_get_cache.return_value = cache_path

        # Mock existence and is_dir to pass initial checks
        with patch.object(Path, "exists", return_value=True), patch.object(Path, "is_dir", return_value=True):
            clear_app_cache()

        # Should NOT call rmtree
        mock_rmtree.assert_not_called()


def test_cleanup_dlq_tmp_removes_old_files(tmp_path, monkeypatch):
    # Setup app root and dlq temp file
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))
    base = get_cache_dir() / "dlq" / "tbl"
    base.mkdir(parents=True, exist_ok=True)
    f = base / "batch_123.ipc.tmp"
    f.write_bytes(b"x")
    # Set mtime to old
    old_time = time.time() - 1000
    os.utime(f, (old_time, old_time))
    deleted = cleanup_dlq_tmp(base=base.parent, retention_s=1)
    assert deleted >= 1
    assert not f.exists()


# ===== Additional utils/redact tests (merged) =====


@pytest.mark.parametrize(
    ("val", "expected"),
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
        ("junk", None),
        (None, None),
    ],
)
def test_env_bool_variants(val: str | None, expected: bool | None, monkeypatch: pytest.MonkeyPatch) -> None:
    if val is None:
        monkeypatch.delenv("X_BOOL", raising=False)
    else:
        monkeypatch.setenv("X_BOOL", val)
    got = env_bool("X_BOOL", default=None)  # type: ignore[arg-type]
    assert got is expected


@pytest.mark.parametrize(("val", "expected"), [("10", 10), (" -7 ", -7), ("x", None), (None, None)])
def test_env_int_variants(val: str | None, expected: int | None, monkeypatch: pytest.MonkeyPatch) -> None:
    if val is None:
        monkeypatch.delenv("X_INT", raising=False)
    else:
        monkeypatch.setenv("X_INT", val)
    assert env_int("X_INT") == expected


@pytest.mark.parametrize(("val", "expected"), [("3.14", 3.14), (" -2 ", -2.0), ("x", None), (None, None)])
def test_env_float_variants(val: str | None, expected: float | None, monkeypatch: pytest.MonkeyPatch) -> None:
    if val is None:
        monkeypatch.delenv("X_FLOAT", raising=False)
    else:
        monkeypatch.setenv("X_FLOAT", val)
    assert env_float("X_FLOAT") == expected


def test_validate_tickers_ok_and_errors() -> None:
    validate_tickers(["AAPL", "AAPL", "MSFT"])
    with pytest.raises(InputError):
        validate_tickers([])  # type: ignore[arg-type]
    with pytest.raises(InputError):
        validate_tickers([" "])  # type: ignore[arg-type]
    with pytest.raises(InputError):
        validate_tickers([123])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("msg", "expected"),
    [
        ("fetch https://api.example.com?q=1", "fetch [redacted]"),
        ("no url here", "no url here"),
        ("multiple https://a/b?x=1 and http://b/c", "multiple [redacted] and [redacted]"),
    ],
)
def test_redact_urls(msg: str, expected: str) -> None:
    assert _redact_urls(msg) == expected


def test_set_env_and_load_process(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("X_A", raising=False)
    monkeypatch.setenv("X_B", "old")
    set_env({"X_A": "1", "X_B": None})
    assert os.getenv("X_A") == "1"
    assert os.getenv("X_B") is None
    monkeypatch.delenv("TICKS", raising=False)
    assert load_tickers_env("TICKS", ["A"]) == ["A"]
    monkeypatch.setenv("TICKS", " aapl , msft ,, nvda ")
    assert load_tickers_env("TICKS", []) == ["AAPL", "MSFT", "NVDA"]
    assert process_symbols(None) is None
    assert process_symbols([" a ", "B"]) == ["A", "B"]


def test_load_tickers_env_empty_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("Z_TICKS", "")
    assert load_tickers_env("Z_TICKS", ["DEF"]) == ["DEF"]


def test_load_tickers_env_duplicates_and_case(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("Z_TICKS", "aapl,msft,aapl, ,nvda")
    ticks = load_tickers_env("Z_TICKS", [])
    assert ticks == ["AAPL", "MSFT", "AAPL", "NVDA"]


def test_check_memory_safety_paths() -> None:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        check_memory_safety(
            estimated_size=800 * 1024 * 1024,
            available_memory=1024 * 1024 * 1024,
            num_tickers=100,
            threshold_ratio=0.5,
        )
        assert any("High memory usage" in str(x.message) for x in w)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        check_memory_safety(
            estimated_size=3 * 1024 * 1024 * 1024,
            available_memory=10 * 1024 * 1024 * 1024,
            num_tickers=100,
            threshold_ratio=0.9,
            threshold_absolute=2 * 1024 * 1024 * 1024,
        )
        assert any("Large data request" in str(x.message) for x in w)


def test_validate_memory_usage_invokes_check(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_psutil = types.SimpleNamespace(virtual_memory=lambda: types.SimpleNamespace(available=1024 * 1024 * 1024))
    monkeypatch.setattr("vertex_forager.utils.psutil", fake_psutil, raising=False)
    called = {"ok": False}

    def _check(estimated_size: int, available_memory: int, num_tickers: int, **_: object) -> None:
        called["ok"] = True
        assert estimated_size > 0
        assert available_memory > 0
        assert num_tickers == 2

    monkeypatch.setattr("vertex_forager.utils.check_memory_safety", _check, raising=False)
    validate_memory_usage(symbols=["A", "B"], connect_db=None, bytes_per_item=1024)
    assert called["ok"] is True


def test_validate_memory_usage_invalid_bytes_per_item() -> None:
    with pytest.raises(ValueError, match="bytes_per_item must be a positive integer"):
        validate_memory_usage(symbols=["A"], connect_db=None, bytes_per_item=0)


def test_cleanup_dlq_tmp_default_base(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(tmp_path / "app"))
    # create default dlq tmp under get_cache_dir()
    from vertex_forager.utils import get_cache_dir as _gcd

    dlq = _gcd() / "dlq" / "tbl"
    dlq.mkdir(parents=True, exist_ok=True)
    tmpf = dlq / "x.ipc.tmp"
    tmpf.write_bytes(b"x")
    deleted = cleanup_dlq_tmp(base=None, retention_s=0)
    assert deleted >= 1


def test_as_dict_none_and_object() -> None:
    assert as_dict(None) == {}

    class R:
        def __init__(self) -> None:
            self.metrics_counters = {"x": 1}
            self.metrics_histograms = {}
            self.metrics_summary = {"t": 2}
            self.tables = {"a": {}}
            self.errors = []

    d = as_dict(R())
    assert d["counters"]["x"] == 1
    assert "summary" in d
    assert "tables" in d
    assert "errors" in d


def test_as_dict_serializes_runerror() -> None:
    class R:
        def __init__(self) -> None:
            self.metrics_counters = {}
            self.metrics_histograms = {}
            self.metrics_summary = {}
            self.tables = {}
            self.errors = [
                RunError(
                    provider="yfinance",
                    dataset="price",
                    symbol="AAPL",
                    exc_type="peewee.OperationalError",
                    message="database is locked",
                    retryable=False,
                )
            ]

    d = as_dict(R())
    assert isinstance(d["errors"][0], dict)
    assert d["errors"][0]["provider"] == "yfinance"
    assert d["errors"][0]["dataset"] == "price"
    assert d["errors"][0]["symbol"] == "AAPL"
    assert d["errors"][0]["exc_type"] == "peewee.OperationalError"
    assert d["errors"][0]["message"] == "database is locked"
    assert d["errors"][0]["retryable"] is False


@pytest.mark.parametrize(
    ("raw_errors", "expected"),
    [
        (None, []),
        ("single-error", ["single-error"]),
        ({"code": "E1"}, ["{'code': 'E1'}"]),
        (123, ["123"]),
    ],
)
def test_as_dict_normalizes_errors_input(raw_errors: object, expected: list[str]) -> None:
    class R:
        def __init__(self, errors_value: object) -> None:
            self.metrics_counters = {}
            self.metrics_histograms = {}
            self.metrics_summary = {}
            self.tables = {}
            self.errors = errors_value

    d = as_dict(R(raw_errors))
    assert d["errors"] == expected


def test_compact_level_formatter_and_list_handler() -> None:
    import logging

    logger = logging.getLogger("vf-test")
    logger.setLevel(logging.DEBUG)
    handler = ListHandler()
    fmt = CompactLevelFormatter("%(levelname)s:%(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    try:
        logger.debug("dbg")
        logger.warning("hello")
        assert handler.records
        # The first record may be debug, ensure both Debug and Warning are present
        levels = {fmt.format(r).split(":", 1)[0] for r in handler.records}
        assert "Debug" in levels
        assert "Warning" in levels

        handler.records.clear()
        logger.info("world")
        assert handler.records
        formatted2 = fmt.format(handler.records[0])
        assert formatted2.startswith("Info:")
        handler.records.clear()
        logger.error("boom")
        assert handler.records
        formatted3 = fmt.format(handler.records[0])
        assert formatted3.startswith("Error:")
    finally:
        logger.removeHandler(handler)
        handler.close()


def test_check_memory_safety_invalid_thresholds() -> None:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        check_memory_safety(
            estimated_size=10 * 1024 * 1024,  # 10MB
            available_memory=1024 * 1024 * 1024,  # 1GB
            num_tickers=1,
            threshold_ratio=-1,
            threshold_absolute=None,
        )
        check_memory_safety(
            estimated_size=10 * 1024 * 1024,
            available_memory=1024 * 1024 * 1024,
            num_tickers=1,
            threshold_ratio=2,
            threshold_absolute=0,
        )
        assert len(w) == 2
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        check_memory_safety(
            estimated_size=1,  # tiny
            available_memory=1024 * 1024 * 1024,
            num_tickers=1,
            threshold_ratio=0.9999,
        )
        assert not w


def test_cleanup_dlq_tmp_negative_retention_raises(tmp_path: Path) -> None:
    base = tmp_path / "dlq"
    base.mkdir()
    with pytest.raises(ValueError, match="cleanup_dlq_tmp: retention_s must be non-negative"):
        cleanup_dlq_tmp(base=base, retention_s=-1)


def test_sanitize_field_cases() -> None:
    assert sanitize_field(None) == ""
    assert sanitize_field("  a  b\tc  ") == "a_b_c"
    assert sanitize_field("x==y") == "x_y"
    assert sanitize_field("  __  ") == ""


def test_load_env_file_sets_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    envp = tmp_path / ".env"
    envp.write_text("VF_TEST_KEY=hello\n")
    monkeypatch.delenv("VF_TEST_KEY", raising=False)
    load_env_file(envp)
    assert os.getenv("VF_TEST_KEY") == "hello"


def test_get_app_root_with_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "vfroot"
    monkeypatch.setenv("VERTEXFORAGER_ROOT", str(base))
    root = get_app_root()
    assert root == base
    assert root.exists()
    assert root.is_dir()


def test_jupyter_safe_sync_and_async_contexts() -> None:
    async def _afunc(x: int) -> int:
        return x + 1

    safe = jupyter_safe(_afunc)
    # Sync context (no running loop)
    assert safe(1) == 2

    # Async context: call inside a running loop
    import asyncio

    async def _runner():
        return safe(2)

    import importlib.util

    if importlib.util.find_spec("nest_asyncio") is None:
        with pytest.raises(RuntimeError) as excinfo:
            asyncio.run(_runner())
        assert "Running inside an event loop" in str(excinfo.value)
    else:
        result = asyncio.run(_runner())
        assert result == 3


def test_ipython_helpers_do_not_raise() -> None:
    # These helpers are best-effort and should not raise in non-notebook environments
    _ipython_display("hello")
    _safe_get_ipython()


def test_pbar_updater_pagination_and_final():
    mock_pbar = MagicMock(spec=tqdm)
    updater = create_pbar_updater(mock_pbar)
    updater(
        ProgressSnapshot(
            jobs_done=1,
            jobs_total=2,
            pct=50.0,
            throughput_sym_per_s=1.0,
            eta_s=1.0,
            errors=0,
            retries=0,
            rows_written=1,
            elapsed_s=1.0,
            active_workers=1,
            pending_jobs=1,
            throttle_events=0,
            dlq_spooled=0,
            memory_mb=100.0,
            cpu_pct=5.0,
        )
    )
    mock_pbar.set_postfix.assert_called()

    updater(
        ProgressSnapshot(
            jobs_done=2,
            jobs_total=2,
            pct=100.0,
            throughput_sym_per_s=1.2,
            eta_s=0.0,
            errors=0,
            retries=0,
            rows_written=2,
            elapsed_s=2.0,
            active_workers=0,
            pending_jobs=0,
            throttle_events=0,
            dlq_spooled=0,
            memory_mb=100.0,
            cpu_pct=5.0,
            finished=True,
        )
    )
    mock_pbar.update.assert_called()
    mock_pbar.close.assert_called_once()
