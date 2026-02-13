from __future__ import annotations

from typing import Any, Protocol, TYPE_CHECKING, Generic, TypeVar
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

    def register(self, key: str, item: T) -> None:
        """Register an item with a specific key.

        Args:
            key: Unique identifier for the item.
            item: The item to register.

        Raises:
            ValueError: If the key is already registered.
        """
        if key in self._registry:
            raise ValueError(f"Key already registered in {self._name}: {key}")
        self._registry[key] = item

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
        return list(self._registry.keys())


# =============================================================================
# Writer Registry
# =============================================================================


class WriterFactory(Protocol):
    """Protocol for writer factory functions."""

    def __call__(self, uri: str) -> "BaseWriter": ...


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
        api_key: str,
        rate_limit: int,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs: Any,
    ) -> "BaseRouter": ...


@dataclass(frozen=True, slots=True)
class RouterRegistration:
    """Metadata for a registered provider router."""

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
        self, api_key: str, rate_limit: int, **kwargs: Any
    ) -> "BaseClient": ...


@dataclass(frozen=True, slots=True)
class ClientRegistration:
    """Metadata for a registered provider client."""

    env_api_key: str | None
    factory: ClientFactory


# Registry for Client Registrations
# Key: Provider name (e.g., "sharadar")
clients = Registry[ClientRegistration]("client")
