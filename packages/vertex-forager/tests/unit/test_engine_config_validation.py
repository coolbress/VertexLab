from pydantic import ValidationError
import pytest

from vertex_forager.core.config import (
    AdaptiveThrottleConfig,
    HTTPConfig,
    ResolvedClientConfig,
    RetryConfig,
    StorageConfig,
)


def test_runtime_config_requests_per_minute_positive() -> None:
    with pytest.raises(ValueError, match=r".*"):
        ResolvedClientConfig(requests_per_minute=0, concurrency=None, retry=RetryConfig()).assert_valid()
    cfg = ResolvedClientConfig(requests_per_minute=60, concurrency=None, retry=RetryConfig())
    cfg.assert_valid()


def test_runtime_config_concurrency_validation() -> None:
    with pytest.raises(ValueError, match=r".*"):
        ResolvedClientConfig(
            requests_per_minute=60,
            concurrency=0,
            retry=RetryConfig(),
        ).assert_valid()
    with pytest.raises(ValueError, match=r".*"):
        ResolvedClientConfig(
            requests_per_minute=60,
            concurrency=-1,
            retry=RetryConfig(),
        ).assert_valid()
    ResolvedClientConfig(
        requests_per_minute=60,
        concurrency=1,
        retry=RetryConfig(),
    ).assert_valid()
    ResolvedClientConfig(
        requests_per_minute=60,
        concurrency=None,
        retry=RetryConfig(),
    ).assert_valid()


def test_runtime_config_quality_check_validation_after_mutation() -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, retry=RetryConfig())
    cfg.quality_check = "typo"  # type: ignore[assignment]

    with pytest.raises(ValueError, match="quality_check"):
        cfg.assert_valid()


def test_grouped_public_configs_forbid_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        AdaptiveThrottleConfig(extra_field=True)
    with pytest.raises(ValidationError):
        HTTPConfig(extra_field=True)
    with pytest.raises(ValidationError):
        StorageConfig(extra_field=True)


def test_storage_config_retention_days_default() -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, retry=RetryConfig())
    assert cfg.storage.checkpoint_retention_days == 7
    assert cfg.storage.run_history_retention_days == 90


def test_storage_config_custom_retention_days() -> None:
    cfg = ResolvedClientConfig(
        requests_per_minute=60,
        retry=RetryConfig(),
        storage=StorageConfig(checkpoint_retention_days=30, run_history_retention_days=180),
    )
    assert cfg.storage.checkpoint_retention_days == 30
    assert cfg.storage.run_history_retention_days == 180


def test_storage_config_flush_threshold_rows_default() -> None:
    cfg = ResolvedClientConfig(requests_per_minute=60, retry=RetryConfig())
    assert cfg.storage.flush_threshold_rows > 0
