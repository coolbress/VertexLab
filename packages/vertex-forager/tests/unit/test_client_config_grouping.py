from __future__ import annotations

from collections.abc import Iterator, Mapping
import importlib.util
import logging
import warnings

import pytest

import vertex_forager
from vertex_forager import (
    AdaptiveThrottleConfig,
    HTTPConfig,
    RetryConfig,
    SchedulerConfig,
    create_client,
)
import vertex_forager.clients.base as base_mod
from vertex_forager.constants import HTTP_TIMEOUT_S

if importlib.util.find_spec("yfinance") is None:
    YFinanceClient = None
else:
    from vertex_forager.providers.yfinance.client import YFinanceClient

pytestmark = pytest.mark.skipif(
    YFinanceClient is None,
    reason="requires optional dependency: vertex-forager[yfinance]",
)


def test_create_client_accepts_grouped_public_configs() -> None:
    client = create_client(
        provider="yfinance",
        rate_limit=120,
        schedule=SchedulerConfig(quantum=3),
        retry=RetryConfig(max_attempts=5),
        throttle=AdaptiveThrottleConfig(enabled=True, window_s=30, recovery_factor=0.05),
        concurrency=4,
        flush_threshold_rows=10_000,
        checkpoint_retention_days=5,
        run_history_retention_days=45,
        http_timeout_s=15.0,
        limits=HTTPConfig(max_connections=50, max_keepalive_connections=25),
    )

    assert isinstance(client, YFinanceClient)
    assert client.config.requests_per_minute == 120
    assert client.config.schedule.quantum == 3
    assert client.config.retry.max_attempts == 5
    assert client.config.adaptive_throttle_enabled is True
    assert client.config.adaptive_throttle_window_s == 30
    assert client.config.recovery_factor == 0.05
    assert client.config.concurrency == 4
    assert client.config.flush_threshold_rows == 10_000
    assert client.config.checkpoint_retention_days == 5
    assert client.config.run_history_retention_days == 45
    assert client._http_timeout_s == 15.0
    assert client._http_limits.max_connections == 50
    assert client._http_limits.max_keepalive_connections == 25


def test_create_client_schedule_defaults_are_applied() -> None:
    client = create_client(provider="yfinance", rate_limit=60)

    assert isinstance(client, YFinanceClient)
    assert client.config.schedule == SchedulerConfig()
    assert client.config.quantum == 3
    assert client.config.max_pending_per_symbol is None
    assert client.config.backpressure_threshold is None


def test_removed_pagination_max_burst_kwarg_is_rejected() -> None:
    with pytest.raises(TypeError):
        create_client(provider="yfinance", rate_limit=60, pagination_max_burst=3)


def test_removed_legacy_adaptive_throttle_kwargs_are_rejected() -> None:
    with pytest.raises(TypeError):
        create_client(
            provider="yfinance",
            rate_limit=60,
            adaptive_throttle_enabled=True,
        )


@pytest.mark.parametrize(
    "removed_kwargs",
    [
        {"otel_enabled": True},
        {"tracer": object()},
        {"advanced": object()},
    ],
)
def test_removed_legacy_advanced_kwargs_are_rejected(removed_kwargs: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        create_client(provider="yfinance", rate_limit=60, **removed_kwargs)


@pytest.mark.parametrize(
    "removed_kwargs",
    [
        {"structured_logs": True},
        {"log_verbose": True},
    ],
)
def test_removed_runtime_kwargs_are_rejected(removed_kwargs: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        create_client(provider="yfinance", rate_limit=60, **removed_kwargs)


@pytest.mark.parametrize(
    "removed_kwargs",
    [
        {"metrics_enabled": True},
        {"dlq_enabled": False},
        {"writer_chunk_rows": 20_000},
        {"writer_concurrency": 2},
    ],
)
def test_other_removed_runtime_kwargs_are_rejected(removed_kwargs: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        create_client(provider="yfinance", rate_limit=60, **removed_kwargs)


def test_removed_logging_kwargs_are_rejected_by_provider_constructor() -> None:
    with pytest.raises(TypeError):
        YFinanceClient(rate_limit=60, structured_logs=True)  # type: ignore[call-arg]


def test_vertex_forager_root_logger_has_null_handler() -> None:
    logger = logging.getLogger("vertex_forager")
    assert any(isinstance(handler, logging.NullHandler) for handler in logger.handlers)
    assert vertex_forager.__version__


def test_non_auth_env_vars_no_longer_backfill_client_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "11")

    client = create_client(provider="yfinance", rate_limit=60)

    assert isinstance(client, YFinanceClient)
    assert client._http_timeout_s == HTTP_TIMEOUT_S
    assert client._http_limits.max_connections == HTTPConfig().max_connections
    assert client._http_limits.max_keepalive_connections == HTTPConfig().max_keepalive_connections


def test_client_creation_ignores_non_auth_env_vars_without_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "11")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        client = create_client(provider="yfinance", rate_limit=60)

    assert isinstance(client, YFinanceClient)
    assert client._http_timeout_s == HTTP_TIMEOUT_S
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_create_client_rejects_removed_retry_flag_in_mapping_subclass() -> None:
    class _RetryMapping(Mapping[str, object]):
        def __iter__(self) -> Iterator[str]:
            yield from ("enable_http_status_retry", "retry_status_codes")

        def __len__(self) -> int:
            return 2

        def __getitem__(self, key: str) -> object:
            values = {
                "enable_http_status_retry": False,
                "retry_status_codes": (429, 503),
            }
            return values[key]

    with pytest.raises(ValueError, match="has been removed"):
        create_client(provider="yfinance", rate_limit=60, retry=_RetryMapping())


def test_build_http_client_uses_normalized_defaults_without_rereading_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "99")
    monkeypatch.setenv("VF_HTTP_MAX_CONNECTIONS", "321")
    monkeypatch.setenv("VF_HTTP_MAX_KEEPALIVE", "123")

    captured: dict[str, float | int] = {}

    def _fake_build_async_client(*, timeout_s: float, max_keepalive_connections: int, max_connections: int):
        captured["timeout_s"] = timeout_s
        captured["max_keepalive_connections"] = max_keepalive_connections
        captured["max_connections"] = max_connections
        return object()

    monkeypatch.setattr(base_mod, "build_async_client", _fake_build_async_client)

    client = create_client(
        provider="yfinance",
        rate_limit=60,
        http_timeout_s=HTTP_TIMEOUT_S,
        limits=HTTPConfig(),
    )

    built = client._build_http_client()

    assert built is not None
    assert captured == {
        "timeout_s": HTTP_TIMEOUT_S,
        "max_keepalive_connections": HTTPConfig().max_keepalive_connections,
        "max_connections": HTTPConfig().max_connections,
    }


def test_rpm_floor_ratio_stored_in_config_and_resolves_in_controller() -> None:
    client = create_client(
        provider="yfinance",
        rate_limit=60,
        throttle=AdaptiveThrottleConfig(rpm_floor_ratio=0.10),
    )
    assert client.config.rpm_floor_ratio == 0.10
    assert client.controller._rpm_floor == 6
