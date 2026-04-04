"""Client implementations for different data providers."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from vertex_forager.clients.base import BaseClient
from vertex_forager.constants import DEFAULT_RATE_LIMIT
from vertex_forager.core.registries import (
    ClientRegistration,
)
from vertex_forager.core.registries import (
    clients as client_registry,
)

if TYPE_CHECKING:
    from vertex_forager.core.config import (
        AdaptiveThrottleConfig,
        HTTPConfig,
        RetryConfig,
        SchedulerConfig,
        StorageConfig,
    )
    from vertex_forager.providers.sharadar.client import SharadarClient
    from vertex_forager.providers.yfinance.client import YFinanceClient


_UNSET = object()


def _register_sharadar() -> None:
    from vertex_forager.providers.sharadar.client import SharadarClient

    def _sharadar_factory(
        *,
        api_key: str | None = None,
        rate_limit: int,
        schedule: SchedulerConfig | dict[str, Any] | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        quality_check: Literal["warn", "error"] = "warn",
        concurrency: int | None = None,
        storage: StorageConfig | dict[str, Any] | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
        **_kwargs: Any,
    ) -> BaseClient:
        return SharadarClient(
            api_key=api_key or "",
            rate_limit=rate_limit,
            schedule=schedule,
            retry=retry,
            throttle=throttle,
            quality_check=quality_check,
            concurrency=concurrency,
            storage=storage,
            limits=limits,
        )

    # Register known providers
    client_registry.register(
        "sharadar",
        ClientRegistration(
            env_api_key="SHARADAR_API_KEY",  # pragma: allowlist secret (variable name only)
            factory=_sharadar_factory,
        ),
    )


def _register_yfinance() -> None:
    from vertex_forager.providers.yfinance.client import YFinanceClient

    def _yfinance_factory(
        *,
        api_key: str | None = None,
        rate_limit: int,
        schedule: SchedulerConfig | dict[str, Any] | None = None,
        retry: RetryConfig | dict[str, Any] | None = None,
        throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
        quality_check: Literal["warn", "error"] = "warn",
        pickle_compat_datasets: list[str] | None = None,
        concurrency: int | None = None,
        storage: StorageConfig | dict[str, Any] | None = None,
        limits: HTTPConfig | dict[str, Any] | None = None,
        **_kwargs: Any,
    ) -> BaseClient:
        return YFinanceClient(
            api_key=api_key or "",
            rate_limit=rate_limit,
            schedule=schedule,
            retry=retry,
            throttle=throttle,
            quality_check=quality_check,
            pickle_compat_datasets=pickle_compat_datasets,
            concurrency=concurrency,
            storage=storage,
            limits=limits,
        )

    client_registry.register(
        "yfinance",
        ClientRegistration(
            env_api_key=None,
            factory=_yfinance_factory,
        ),
    )


@overload
def create_client(
    *,
    provider: Literal["yfinance"],
    schedule: SchedulerConfig | dict[str, Any] | None = None,
    retry: RetryConfig | dict[str, Any] | None = None,
    throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
    quality_check: Literal["warn", "error"] = "warn",
    pickle_compat_datasets: list[str] | None = None,
    concurrency: int | None = None,
    storage: StorageConfig | dict[str, Any] | None = None,
    limits: HTTPConfig | dict[str, Any] | None = None,
) -> YFinanceClient: ...


@overload
def create_client(
    *,
    provider: Literal["sharadar"],
    api_key: str | None = None,
    rate_limit: int,
    schedule: SchedulerConfig | dict[str, Any] | None = None,
    retry: RetryConfig | dict[str, Any] | None = None,
    throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
    quality_check: Literal["warn", "error"] = "warn",
    concurrency: int | None = None,
    storage: StorageConfig | dict[str, Any] | None = None,
    limits: HTTPConfig | dict[str, Any] | None = None,
) -> SharadarClient: ...


def create_client(
    *,
    provider: str,
    api_key: object = _UNSET,
    rate_limit: object = _UNSET,
    schedule: SchedulerConfig | dict[str, Any] | None = None,
    retry: RetryConfig | dict[str, Any] | None = None,
    throttle: AdaptiveThrottleConfig | dict[str, Any] | None = None,
    quality_check: Literal["warn", "error"] = "warn",
    pickle_compat_datasets: list[str] | None = None,
    concurrency: int | None = None,
    storage: StorageConfig | dict[str, Any] | None = None,
    limits: HTTPConfig | dict[str, Any] | None = None,
) -> BaseClient:
    """
    Create and configure a client instance for the specified provider.

    Args:
        provider: The provider identifier (e.g., "sharadar").
        api_key: Provider API key where supported.
        rate_limit: Provider rate limit in requests per minute where supported.
        schedule: Grouped scheduler configuration for always-on DRR fairness.
        retry: Grouped retry policy configuration.
        throttle: Grouped adaptive throttle policy configuration.
        quality_check: Data quality violation handling mode.
        pickle_compat_datasets: YFinance-only pickle compatibility dataset allowlist.
        concurrency: Explicit fetch concurrency limit.
        storage: Grouped data-lifecycle and write-path tuning settings.
        limits: Grouped HTTP connection-pool configuration.

    Returns:
        Configured client instance inheriting from BaseClient.

    Raises:
        ValueError: If API key is missing.
        KeyError: If provider is unknown.
    """
    try:
        registration = client_registry.get(provider)
    except KeyError:
        if provider == "sharadar":
            _register_sharadar()
            registration = client_registry.get(provider)
        elif provider == "yfinance":
            _register_yfinance()
            registration = client_registry.get(provider)
        else:
            raise KeyError(f"Unsupported client: {provider}") from None

    api_key_supplied = api_key is not _UNSET
    rate_limit_supplied = rate_limit is not _UNSET
    resolved_key = None if api_key is _UNSET else cast("str | None", api_key)
    if not resolved_key and registration.env_api_key:
        resolved_key = os.getenv(registration.env_api_key)

    if not resolved_key and registration.env_api_key:
        raise ValueError(f"Missing api_key (set api_key or {registration.env_api_key} in environment/.env)")

    if provider == "yfinance":
        if api_key_supplied:
            raise TypeError("api_key is not supported when provider='yfinance'")
        if rate_limit_supplied:
            raise TypeError("rate_limit is not supported when provider='yfinance'")
        return registration.factory(
            api_key=None,
            rate_limit=DEFAULT_RATE_LIMIT,
            schedule=schedule,
            retry=retry,
            throttle=throttle,
            quality_check=quality_check,
            pickle_compat_datasets=pickle_compat_datasets,
            concurrency=concurrency,
            storage=storage,
            limits=limits,
        )
    if pickle_compat_datasets is not None:
        raise TypeError("pickle_compat_datasets is only supported when provider='yfinance'")
    if not rate_limit_supplied or rate_limit is None:
        raise ValueError(f"Missing rate_limit for provider '{provider}'")
    return registration.factory(
        api_key=resolved_key,
        rate_limit=cast("int", rate_limit),
        schedule=schedule,
        retry=retry,
        throttle=throttle,
        quality_check=quality_check,
        concurrency=concurrency,
        storage=storage,
        limits=limits,
    )


__all__ = ["BaseClient", "create_client"]
