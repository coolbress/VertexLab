from pydantic import ValidationError
import pytest

from vertex_forager.core.config import AdvancedConfig, DownshiftConfig, HTTPConfig, ResolvedClientConfig, RetryConfig


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


def test_grouped_public_configs_forbid_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        DownshiftConfig(extra_field=True)
    with pytest.raises(ValidationError):
        HTTPConfig(extra_field=True)
    with pytest.raises(ValidationError):
        AdvancedConfig(extra_field=True)
