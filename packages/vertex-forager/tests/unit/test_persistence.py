"""Tests for persistence functionality (checkpointing and run history)."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

from vertex_forager.core.checkpoint import (
    Checkpoint,
    atomic_write_json,
    get_cache_dir,
    load_checkpoint,
    save_checkpoint,
    save_run_history,
)
from vertex_forager.core.config import ResolvedClientConfig, RunResult
from vertex_forager.core.errors import RunError


def test_get_cache_dir() -> None:
    """Test that get_cache_dir returns the correct directory."""
    with patch.dict("os.environ", {}, clear=True):
        cache_dir = get_cache_dir()
        assert cache_dir == Path.home() / ".cache" / "vertex-forager"

    # Test with a temporary directory for XDG_CACHE_HOME
    with tempfile.TemporaryDirectory() as tmpdir, patch.dict("os.environ", {"XDG_CACHE_HOME": tmpdir}):
        cache_dir = get_cache_dir()
        expected_dir = Path(tmpdir) / "vertex-forager"
        assert cache_dir == expected_dir


def test_atomic_write_json() -> None:
    """Test atomic JSON writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.json"
        test_data = {"test": "data", "number": 42}

        atomic_write_json(test_data, test_file)

        assert test_file.exists()
        with open(test_file) as f:
            loaded_data = json.load(f)
        assert loaded_data == test_data


def test_checkpoint_model() -> None:
    """Test Checkpoint data model."""
    checkpoint = Checkpoint(
        run_id="test_run_123",
        provider="test_provider",
        dataset="test_dataset",
        completed=["AAPL", "MSFT"],
        failed=["GOOG"]
    )

    assert checkpoint.run_id == "test_run_123"
    assert checkpoint.provider == "test_provider"
    assert checkpoint.dataset == "test_dataset"
    assert checkpoint.completed == ["AAPL", "MSFT"]
    assert checkpoint.failed == ["GOOG"]

    # Test serialization
    data = checkpoint.model_dump()
    assert data["run_id"] == "test_run_123"
    assert data["completed"] == ["AAPL", "MSFT"]


def test_save_and_load_checkpoint() -> None:
    """Test saving and loading checkpoints."""
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "vertex_forager.core.checkpoint.get_cache_dir",
        return_value=Path(tmpdir)
    ):
        checkpoint = Checkpoint(
            run_id="test_run_123",
            provider="test_provider",
            dataset="test_dataset",
            completed=["AAPL", "MSFT"],
            failed=["GOOG"]
        )

        save_checkpoint(checkpoint)

        # Check that file was created
        checkpoint_file = Path(tmpdir) / "checkpoints" / "test_run_123" / "progress.json"
        assert checkpoint_file.exists()

        # Load the checkpoint
        loaded = load_checkpoint("test_run_123")
        assert loaded is not None
        assert loaded.run_id == "test_run_123"
        assert loaded.completed == ["AAPL", "MSFT"]
        assert loaded.failed == ["GOOG"]

        # Test loading non-existent checkpoint
        assert load_checkpoint("non_existent") is None


def test_save_run_history() -> None:
    """Test saving run history."""
    with tempfile.TemporaryDirectory() as tmpdir, patch(
        "vertex_forager.core.checkpoint.get_cache_dir",
        return_value=Path(tmpdir)
    ):
        run_result = RunResult(
            provider="test_provider",
            run_id="test_run_123",
            dataset="test_dataset",
            started_at=1000.0,
            finished_at=1100.0,
            duration_s=100.0,
            coverage_pct=95.5,
            tables={"table1": 100, "table2": 200},
            quality_violations={"table1": 3},
            errors=[
                RunError(
                    provider="test_provider",
                    dataset="test_dataset",
                    symbol="",
                    exc_type="ValueError",
                    message="error1",
                    retryable=False,
                ),
                RunError(
                    provider="test_provider",
                    dataset="test_dataset",
                    symbol="",
                    exc_type="ValueError",
                    message="error2",
                    retryable=False,
                ),
            ],
        )

        save_run_history(run_result, "test_run_123")

        # Check that file was created
        run_file = Path(tmpdir) / "runs" / "test_run_123.json"
        assert run_file.exists()

        # Verify the content
        with open(run_file) as f:
            data = json.load(f)

        assert data["run_id"] == "test_run_123"
        assert data["provider"] == "test_provider"
        assert data["dataset"] == "test_dataset"
        assert data["duration_s"] == 100.0
        assert data["error_count"] == 2
        assert data["tables"]["table1"] == 100
        assert data["tables"]["table2"] == 200
        assert data["quality_violations"]["table1"] == 3
        assert data["errors"][0]["provider"] == "test_provider"
        assert data["errors"][0]["dataset"] == "test_dataset"
        assert data["errors"][0]["symbol"] == ""
        assert data["errors"][0]["exc_type"] == "ValueError"
        assert data["errors"][0]["message"] == "error1"
        assert data["errors"][0]["retryable"] is False


def test_run_result_coerces_legacy_string_errors() -> None:
    run_result = RunResult(
        provider="test_provider",
        errors=["legacy-error"],
    )

    assert len(run_result.errors) == 1
    assert isinstance(run_result.errors[0], RunError)
    assert run_result.errors[0].provider == ""
    assert run_result.errors[0].dataset == ""
    assert run_result.errors[0].symbol == ""
    assert run_result.errors[0].exc_type == "builtins.str"
    assert run_result.errors[0].message == "legacy-error"
    assert run_result.errors[0].retryable is False


def test_resolved_client_config_persist_run_history() -> None:
    """Test ResolvedClientConfig persist_run_history parameter."""
    # Test default value
    config = ResolvedClientConfig(requests_per_minute=60)
    assert config.persist_run_history is True

    # Test explicit True
    config = ResolvedClientConfig(requests_per_minute=60, persist_run_history=True)
    assert config.persist_run_history is True

    # Test explicit False
    config = ResolvedClientConfig(requests_per_minute=60, persist_run_history=False)
    assert config.persist_run_history is False


@patch("vertex_forager.core.checkpoint.atomic_write_json")
def test_save_checkpoint_atomic_write(mock_atomic_write: Mock) -> None:
    """Test that save_checkpoint uses atomic write."""
    checkpoint = Checkpoint(
        run_id="test_run",
        provider="test",
        dataset="test",
        completed=[],
        failed=[]
    )

    save_checkpoint(checkpoint)

    mock_atomic_write.assert_called_once()
    call_args = mock_atomic_write.call_args[0]
    assert call_args[0] == checkpoint.model_dump()
    assert "checkpoints" in str(call_args[1])


@patch("vertex_forager.core.checkpoint.atomic_write_json")
def test_save_run_history_atomic_write(mock_atomic_write: Mock) -> None:
    """Test that save_run_history uses atomic write."""
    run_result = RunResult(
        provider="test_provider",
        run_id="test_run_123",
        dataset="test_dataset",
        started_at=1000.0,
        finished_at=1100.0,
        duration_s=100.0,
    )

    save_run_history(run_result, "test_run_123")

    mock_atomic_write.assert_called_once()
    call_args = mock_atomic_write.call_args[0]
    assert call_args[0]["run_id"] == "test_run_123"
    assert "runs" in str(call_args[1])
