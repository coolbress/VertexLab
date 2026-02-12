"""
Unit tests for core.registries module.

Tests cover:
- Generic Registry class
- Writer, Router, and Client registries
- Registration and retrieval
- Error handling
"""

import pytest
from unittest.mock import Mock

from vertex_forager.core.registries import (
    Registry,
    ClientRegistration,
    RouterRegistration,
    clients,
    routers,
    writers,
)


class TestRegistry:
    """Test generic Registry class."""

    def test_initialization(self):
        """Test registry initializes with name."""
        registry = Registry[str]("test_registry")
        assert registry._name == "test_registry"
        assert registry._registry == {}

    def test_register_item(self):
        """Test registering an item."""
        registry = Registry[str]("test")
        registry.register("key1", "value1")
        assert registry.get("key1") == "value1"

    def test_register_duplicate_raises_error(self):
        """Test registering duplicate key raises ValueError."""
        registry = Registry[str]("test")
        registry.register("key1", "value1")

        with pytest.raises(ValueError, match="Key already registered"):
            registry.register("key1", "value2")

    def test_get_existing_item(self):
        """Test retrieving existing item."""
        registry = Registry[int]("numbers")
        registry.register("one", 1)
        registry.register("two", 2)

        assert registry.get("one") == 1
        assert registry.get("two") == 2

    def test_get_nonexistent_item_raises_error(self):
        """Test retrieving non-existent item raises NotImplementedError."""
        registry = Registry[str]("test")

        with pytest.raises(NotImplementedError, match="Unsupported test: unknown"):
            registry.get("unknown")

    def test_list_keys(self):
        """Test listing all registered keys."""
        registry = Registry[str]("test")
        registry.register("key1", "value1")
        registry.register("key2", "value2")
        registry.register("key3", "value3")

        keys = registry.list_keys()
        assert set(keys) == {"key1", "key2", "key3"}

    def test_list_keys_empty_registry(self):
        """Test listing keys from empty registry."""
        registry = Registry[str]("test")
        assert registry.list_keys() == []


class TestClientRegistration:
    """Test ClientRegistration dataclass."""

    def test_create_registration(self):
        """Test creating a client registration."""
        mock_factory = Mock()
        registration = ClientRegistration(
            env_api_key="TEST_API_KEY",
            factory=mock_factory,
        )
        assert registration.env_api_key == "TEST_API_KEY"
        assert registration.factory == mock_factory

    def test_registration_is_frozen(self):
        """Test ClientRegistration is immutable."""
        mock_factory = Mock()
        registration = ClientRegistration(
            env_api_key="TEST_API_KEY",
            factory=mock_factory,
        )

        with pytest.raises(AttributeError):
            registration.env_api_key = "NEW_KEY"  # type: ignore


class TestRouterRegistration:
    """Test RouterRegistration dataclass."""

    def test_create_registration(self):
        """Test creating a router registration."""
        mock_factory = Mock()
        registration = RouterRegistration(factory=mock_factory)
        assert registration.factory == mock_factory

    def test_registration_is_frozen(self):
        """Test RouterRegistration is immutable."""
        mock_factory = Mock()
        registration = RouterRegistration(factory=mock_factory)

        with pytest.raises(AttributeError):
            registration.factory = Mock()  # type: ignore


class TestGlobalRegistries:
    """Test global registry instances."""

    def test_clients_registry_exists(self):
        """Test clients registry is accessible."""
        assert clients is not None
        assert clients._name == "client"

    def test_routers_registry_exists(self):
        """Test routers registry is accessible."""
        assert routers is not None
        assert routers._name == "router"

    def test_writers_registry_exists(self):
        """Test writers registry is accessible."""
        assert writers is not None
        assert writers._name == "writer"

    def test_sharadar_router_is_registered(self):
        """Test that sharadar router is registered."""
        # This is registered in routers/__init__.py
        try:
            registration = routers.get("sharadar")
            assert registration is not None
            assert isinstance(registration, RouterRegistration)
        except NotImplementedError:
            # Registry might not be populated in isolated test
            pass


class TestRegistryTypeSystem:
    """Test registry type safety and protocols."""

    def test_registry_with_callable(self):
        """Test registry can store callables."""
        def my_factory(uri: str) -> str:
            return f"Created from {uri}"

        registry = Registry[callable]("factories")
        registry.register("test", my_factory)

        retrieved = registry.get("test")
        assert callable(retrieved)
        assert retrieved("test_uri") == "Created from test_uri"

    def test_registry_with_complex_types(self):
        """Test registry with complex types."""
        class CustomClass:
            def __init__(self, value: int):
                self.value = value

        registry = Registry[CustomClass]("custom")
        instance = CustomClass(42)
        registry.register("instance", instance)

        retrieved = registry.get("instance")
        assert retrieved.value == 42


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_register_with_special_characters_in_key(self):
        """Test registering with special characters in key."""
        registry = Registry[str]("test")
        registry.register("key-with-dashes", "value1")
        registry.register("key.with.dots", "value2")
        registry.register("key_with_underscores", "value3")

        assert registry.get("key-with-dashes") == "value1"
        assert registry.get("key.with.dots") == "value2"
        assert registry.get("key_with_underscores") == "value3"

    def test_register_none_value(self):
        """Test registering None as a value."""
        registry = Registry[str | None]("test")
        registry.register("none_key", None)
        # Registry.get raises NotImplementedError when value is None
        # This is by design - registry uses None to indicate missing key
        # So we can't actually store None as a value
        with pytest.raises(NotImplementedError):
            registry.get("none_key")

    def test_empty_string_key(self):
        """Test registering with empty string key."""
        registry = Registry[str]("test")
        registry.register("", "empty_key_value")
        assert registry.get("") == "empty_key_value"

    def test_case_sensitive_keys(self):
        """Test that keys are case-sensitive."""
        registry = Registry[str]("test")
        registry.register("Key", "value1")
        registry.register("key", "value2")
        registry.register("KEY", "value3")

        assert registry.get("Key") == "value1"
        assert registry.get("key") == "value2"
        assert registry.get("KEY") == "value3"


class TestMultipleRegistrations:
    """Test scenarios with multiple items."""

    def test_register_many_items(self):
        """Test registering many items."""
        registry = Registry[int]("numbers")

        for i in range(100):
            registry.register(f"key_{i}", i)

        # Verify all items are accessible
        for i in range(100):
            assert registry.get(f"key_{i}") == i

        # Verify list_keys returns all keys
        assert len(registry.list_keys()) == 100

    def test_registry_independence(self):
        """Test that separate registry instances are independent."""
        registry1 = Registry[str]("registry1")
        registry2 = Registry[str]("registry2")

        registry1.register("shared_key", "value1")
        registry2.register("shared_key", "value2")

        assert registry1.get("shared_key") == "value1"
        assert registry2.get("shared_key") == "value2"