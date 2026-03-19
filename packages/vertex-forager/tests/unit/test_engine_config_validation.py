import pytest

from vertex_forager.core.config import EngineConfig, RetryConfig


def test_engine_config_requests_per_minute_positive() -> None:
    with pytest.raises(ValueError, match=r".*"):
        EngineConfig(
            requests_per_minute=0, concurrency=None, retry=RetryConfig()
        ).assert_valid()
    cfg = EngineConfig(requests_per_minute=60, concurrency=None, retry=RetryConfig())
    cfg.assert_valid()


def test_engine_config_concurrency_validation() -> None:
    with pytest.raises(ValueError, match=r".*"):
        EngineConfig(
            requests_per_minute=60,
            concurrency=0,
            retry=RetryConfig(),
        ).assert_valid()
    with pytest.raises(ValueError, match=r".*"):
        EngineConfig(
            requests_per_minute=60,
            concurrency=-1,
            retry=RetryConfig(),
        ).assert_valid()
    EngineConfig(
        requests_per_minute=60,
        concurrency=1,
        retry=RetryConfig(),
    ).assert_valid()
    EngineConfig(
        requests_per_minute=60,
        concurrency=None,
        retry=RetryConfig(),
    ).assert_valid()
