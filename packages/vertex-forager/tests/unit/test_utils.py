import os
from pathlib import Path
import time
from unittest.mock import MagicMock, patch

from tqdm import tqdm

from vertex_forager.utils import (
    cleanup_dlq_tmp,
    clear_app_cache,
    create_pbar_updater,
    get_cache_dir,
)


class TestPbarUpdater:
    def test_update_pbar_counts_symbols_correctly(self):
        """update_pbar counts symbols in a comma-separated string."""
        mock_pbar = MagicMock(spec=tqdm)
        updater = create_pbar_updater(mock_pbar)

        # Mock job and parse_result
        mock_job = MagicMock()
        mock_job.symbol = "AAPL,MSFT,GOOG"

        # Case 1: Normal batch
        updater(job=mock_job, parse_result=None)
        mock_pbar.update.assert_called_with(3)
        # Verify display string
        args, _ = mock_pbar.set_postfix_str.call_args
        assert "Done: AAPL (+2).." in args[0]

        # Case 2: Empty string in batch (e.g. trailing comma or double comma)
        mock_job.symbol = "AAPL,,MSFT, ,GOOG"
        mock_pbar.reset_mock()
        updater(job=mock_job, parse_result=None)
        # Should count "AAPL", "MSFT", "GOOG" -> 3
        mock_pbar.update.assert_called_with(3)
        args, _ = mock_pbar.set_postfix_str.call_args
        assert "Done: AAPL (+2).." in args[0]

    def test_update_pbar_handles_pagination(self):
        """Test that update_pbar does not update count if pagination is active."""
        mock_pbar = MagicMock(spec=tqdm)
        updater = create_pbar_updater(mock_pbar)

        mock_job = MagicMock()
        mock_job.symbol = "AAPL"

        # Mock parse_result with next_jobs
        mock_parse_result = MagicMock()
        mock_parse_result.next_jobs = ["job2"]

        updater(job=mock_job, parse_result=mock_parse_result)

        # Should NOT call update
        mock_pbar.update.assert_not_called()
        # Should update postfix (inline at right) to show pagination
        mock_pbar.set_postfix_str.assert_called()
        assert "Pagination processing" in mock_pbar.set_postfix_str.call_args[0][0]


class TestCacheUtils:
    @patch("vertex_forager.utils.get_app_root")
    @patch("vertex_forager.utils.get_cache_dir")
    @patch("shutil.rmtree")
    def test_clear_app_cache_safety_check_pass(
        self, mock_rmtree, mock_get_cache, mock_get_root, tmp_path: Path
    ):
        """Test that clear_app_cache proceeds when cache is inside app root."""
        # Setup paths
        # Use resolve() to handle symlinks (e.g. /tmp -> /private/tmp on macOS)
        root_path = (tmp_path / "vertex_root").resolve()
        cache_path = (root_path / "cache").resolve()

        mock_get_root.return_value = root_path
        mock_get_cache.return_value = cache_path

        # Mock existence and is_dir
        with patch.object(Path, "exists", return_value=True), \
            patch.object(Path, "is_dir", return_value=True), \
            patch.object(Path, "mkdir") as mock_mkdir:
            clear_app_cache()

            # Should verify relative_to and call rmtree
            mock_rmtree.assert_called_once_with(cache_path)
            mock_mkdir.assert_called_once()

    @patch("vertex_forager.utils.get_app_root")
    @patch("vertex_forager.utils.get_cache_dir")
    @patch("shutil.rmtree")
    def test_clear_app_cache_safety_check_fail(
        self, mock_rmtree, mock_get_cache, mock_get_root, tmp_path: Path
    ):
        """Test that clear_app_cache aborts when cache is outside app root."""
        # Setup paths that are disjoint
        root_path = (tmp_path / "vertex_root").resolve()
        cache_path = Path("/etc/passwd").resolve()  # Dangerous path!

        mock_get_root.return_value = root_path
        mock_get_cache.return_value = cache_path

        # Mock existence and is_dir to pass initial checks
        with patch.object(Path, "exists", return_value=True), \
            patch.object(Path, "is_dir", return_value=True):
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
