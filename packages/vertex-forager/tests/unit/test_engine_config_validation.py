import pytest

from vertex_forager.core.config import ResolvedClientConfig, RetryConfig


def test_runtime_config_requests_per_minute_positive() -> None:
    with pytest.raises(ValueError, match=r".*"):
        ResolvedClientConfig(
            requests_per_minute=0, concurrency=None, retry=RetryConfig()
        ).assert_valid()
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
