from __future__ import annotations

import importlib.util
import warnings

import pytest

from vertex_forager import AdvancedConfig, DownshiftConfig, HTTPConfig, RetryConfig, create_client
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
        metrics_enabled=True,
        dlq_enabled=False,
        pagination_max_burst=3,
        retry=RetryConfig(max_attempts=5),
        downshift=DownshiftConfig(enabled=True, window_s=30, recovery_step=2),
        concurrency=4,
        flush_threshold_rows=10_000,
        writer_chunk_rows=20_000,
        writer_concurrency=2,
        persist_run_history=False,
        http_timeout_s=15.0,
        limits=HTTPConfig(max_connections=50, max_keepalive_connections=25),
        advanced=AdvancedConfig(otel_enabled=True, mem_threshold_ratio=0.5, mem_threshold_abs_mb=None),
    )

    assert isinstance(client, YFinanceClient)
    assert client.config.requests_per_minute == 120
    assert client.config.metrics_enabled is True
    assert client.config.dlq_enabled is False
    assert client.config.pagination_max_burst == 3
    assert client.config.retry.max_attempts == 5
    assert client.config.downshift_enabled is True
    assert client.config.downshift_window_s == 30
    assert client.config.recovery_step == 2
    assert client.config.concurrency == 4
    assert client.config.flush_threshold_rows == 10_000
    assert client.config.writer_chunk_rows == 20_000
    assert client.config.writer_concurrency == 2
    assert client.config.persist_run_history is False
    assert client.config.otel_enabled is True
    assert client._http_timeout_s == 15.0
    assert client._http_limits.max_connections == 50
    assert client._http_limits.max_keepalive_connections == 25
    assert client._memory_threshold_ratio == 0.5
    assert client._memory_threshold_absolute is None


def test_legacy_flat_kwargs_emit_deprecation_and_normalize() -> None:
    with pytest.deprecated_call():
        client = create_client(
            provider="yfinance",
            rate_limit=60,
            downshift_enabled=True,
            downshift_window_s=10,
            error_rate_threshold=0.1,
            otel_enabled=True,
        )

    assert isinstance(client, YFinanceClient)
    assert client.config.downshift_enabled is True
    assert client.config.downshift_window_s == 10
    assert client.config.error_rate_threshold == 0.1
    assert client.config.otel_enabled is True


def test_legacy_persist_run_history_string_false_normalizes_correctly() -> None:
    with pytest.deprecated_call():
        client = create_client(
            provider="yfinance",
            rate_limit=60,
            persist_run_history="false",  # type: ignore[arg-type]
        )

    assert isinstance(client, YFinanceClient)
    assert client.config.persist_run_history is False


def test_string_flag_inputs_normalize_correctly() -> None:
    client = create_client(
        provider="yfinance",
        rate_limit=60,
        metrics_enabled="false",  # type: ignore[arg-type]
        structured_logs="0",  # type: ignore[arg-type]
        log_verbose="no",  # type: ignore[arg-type]
        dlq_enabled="true",  # type: ignore[arg-type]
        persist_run_history="1",  # type: ignore[arg-type]
    )

    assert isinstance(client, YFinanceClient)
    assert client.config.metrics_enabled is False
    assert client.config.structured_logs is False
    assert client.config.log_verbose is False
    assert client.config.dlq_enabled is True
    assert client.config.persist_run_history is True


def test_deprecated_env_vars_still_apply_during_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VF_CONCURRENCY", "7")
    monkeypatch.setenv("VF_FLUSH_THRESHOLD_ROWS", "12000")
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "11")
    monkeypatch.setenv("VF_HTTP_MAX_CONNECTIONS", "30")
    monkeypatch.setenv("VF_HTTP_MAX_KEEPALIVE", "12")
    monkeypatch.setenv("VF_OTEL_ENABLED", "1")
    monkeypatch.setenv("VF_MEM_THRESHOLD_RATIO", "0.4")
    monkeypatch.setenv("VF_MEM_THRESHOLD_ABS_MB", "2048")

    with pytest.deprecated_call():
        client = create_client(provider="yfinance", rate_limit=60)

    assert isinstance(client, YFinanceClient)
    assert client.config.concurrency == 7
    assert client.config.flush_threshold_rows == 12_000
    assert client.config.otel_enabled is True
    assert client._http_timeout_s == 11.0
    assert client._http_limits.max_connections == 30
    assert client._http_limits.max_keepalive_connections == 12
    assert client._memory_threshold_ratio == 0.4
    assert client._memory_threshold_absolute == 2048 * 1024 * 1024


def test_missing_env_vars_do_not_trigger_backfill_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(base_mod.os.environ):
        if name.startswith("VF_"):
            monkeypatch.delenv(name, raising=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        client = create_client(provider="yfinance", rate_limit=60)

    assert isinstance(client, YFinanceClient)
    assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


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


def test_invalid_cross_field_settings_fail_during_client_creation() -> None:
    with pytest.raises(ValueError, match="rpm_floor must be <= requests_per_minute"):
        create_client(
            provider="yfinance",
            rate_limit=60,
            downshift=DownshiftConfig(rpm_floor=100),
        )
