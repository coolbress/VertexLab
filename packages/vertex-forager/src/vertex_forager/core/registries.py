"""Component Registries Module.

This module provides a generic Registry class and specific registry instances for
writers, routers, and clients. It facilitates a plugin-like architecture where
components can be dynamically registered and retrieved by name.

Classes:
    Registry: Generic registry implementation.
    RouterRegistration: Metadata for registered routers.
    ClientRegistration: Metadata for registered clients.
    WriterFactory, RouterFactory, ClientFactory: Protocols for factories.

Usage:
    @routers.register("sharadar")
    def create_sharadar_router(...): ...
"""
from __future__ import annotations

from typing import Any, Protocol, TYPE_CHECKING, Generic, TypeVar, Callable, overload
from dataclasses import dataclass

if TYPE_CHECKING:
    from vertex_forager.writers.base import BaseWriter
    from vertex_forager.routers.base import BaseRouter
    from vertex_forager.clients.base import BaseClient


T = TypeVar("T")


class Registry(Generic[T]):
    """A generic registry for managing component factories or metadata.

    This class provides a Pythonic way to register and retrieve components using
    decorators or explicit registration methods. It replaces repetitive
    dictionary-based registry patterns across the codebase.

    Type Vars:
        T: The type of the registered object (e.g., Factory Protocol, DataClass).
    """

    def __init__(self, name: str) -> None:
        """Initialize the registry.

        Args:
            name: Name of the registry (for error messages).
        """
        self._name = name
        self._registry: dict[str, T] = {}

    @overload
    def register(self, key: str, item: T) -> None: ...

    @overload
    def register(self, key: str, item: None = ...) -> Callable[[T], T]: ...

    def register(self, key: str, item: T | None = None) -> Callable[[T], T] | None:
        """Register an item with a specific key.

        Can be used as a method or a decorator.

        Args:
            key: Unique identifier for the item.
            item: The item to register. If None, returns a decorator.

        Returns:
            A decorator function if item is None, otherwise None.

        Raises:
            ValueError: If the key is already registered.
        """

        def _register(obj: T) -> T:
            if key in self._registry:
                raise ValueError(f"Key already registered in {self._name}: {key}")
            self._registry[key] = obj
            return obj

        if item is None:
            return _register

        _register(item)
        return None

    def get(self, key: str) -> T:
        """Retrieve an item by key.

        Args:
            key: The identifier to look up.

        Returns:
            The registered item.

        Raises:
            KeyError: If the key is not found.
        """
        if key not in self._registry:
            raise KeyError(f"Unsupported {self._name}: {key}")
        return self._registry[key]

    def list_keys(self) -> list[str]:
        """List all registered keys."""
        return list(self._registry)

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the registry."""
        return key in self._registry

    def __len__(self) -> int:
        """Return the number of registered items."""
        return len(self._registry)


# =============================================================================
# Writer Registry
# =============================================================================


class WriterFactory(Protocol):
    """Protocol for writer factory functions."""

    def __call__(self, uri: str) -> "BaseWriter":
        """Create a writer instance.

        Args:
            uri: Connection URI for the writer (e.g., "duckdb:///path/to/db").

        Returns:
            BaseWriter: An initialized writer instance.
        """
        ...


# Registry for Writer Factories
# Key: URI scheme (e.g., "duckdb", "memory")
writers = Registry[WriterFactory]("writer")


# =============================================================================
# Router Registry
# =============================================================================


class RouterFactory(Protocol):
    """Protocol for router factory functions or classes."""

    def __call__(
        self,
        *,
        api_key: str | None,
        rate_limit: int,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> "BaseRouter":
        """Create a router instance.

        Args:
            api_key: API key for the provider, or None if not required.
            rate_limit: Rate limit in requests per minute.
            start_date: Optional start date filter (YYYY-MM-DD).
            end_date: Optional end date filter (YYYY-MM-DD).
            **kwargs: Additional provider-specific arguments.

        Returns:
            BaseRouter: An initialized router instance.
        """
        ...


@dataclass(frozen=True, slots=True)
class RouterRegistration:
    """Metadata for a registered provider router.

    Attributes:
        factory: Factory callable that creates a router instance.
    """

    factory: RouterFactory


# Registry for Router Registrations
# Key: Provider name (e.g., "sharadar")
routers = Registry[RouterRegistration]("router")


# =============================================================================
# Client Registry
# =============================================================================


class ClientFactory(Protocol):
    """Protocol for client factory functions or classes."""

    def __call__(
        self, *, api_key: str | None = None, rate_limit: int, **kwargs: Any
    ) -> "BaseClient":
        """Create a client instance.

        Args:
            api_key: API key for the provider, or None if not required.
            rate_limit: Rate limit in requests per minute.
            **kwargs: Additional provider-specific arguments.

        Returns:
            BaseClient: An initialized client instance.
        """
        ...


@dataclass(frozen=True, slots=True)
class ClientRegistration:
    """Metadata for a registered provider client.

    Attributes:
        env_api_key: Name of the environment variable containing the API key,
            or None if the provider does not require an API key.
        factory: Factory callable that creates a client instance.
    """

    env_api_key: str | None
    factory: ClientFactory


# Registry for Client Registrations
# Key: Provider name (e.g., "sharadar")
clients = Registry[ClientRegistration]("client")
