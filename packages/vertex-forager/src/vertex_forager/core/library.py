from __future__ import annotations

from typing import Any, Protocol
from vertex_forager.core.config import RequestSpec
from vertex_forager.core.types import JSONValue


class LibraryFetcher(Protocol):
    """Protocol for provider-specific library fetchers."""
    scheme: str

    def fetch(self, spec: RequestSpec) -> Any: ...


class BaseLibraryFetcher:
    """Base class providing common parsing helpers for library fetchers."""
    scheme: str = ""

    def parse_spec(self, spec: RequestSpec) -> tuple[str, str, dict[str, JSONValue]]:
        """Extract dataset, payload, and lib dict from RequestSpec with validation."""
        if "://" not in spec.url:
            raise ValueError("Library URL must contain scheme separator '://'")
        scheme, payload = spec.url.split("://", 1)
        if self.scheme and scheme != self.scheme:
            raise ValueError(f"Unsupported library scheme: {scheme}")
        params = spec.params
        dataset = params.get("dataset", "price")
        lib = params.get("lib")
        if not isinstance(lib, dict):
            raise ValueError("Missing library call specification ('lib') in request params")
        return payload, str(dataset), dict(lib)


_REGISTRY: dict[str, LibraryFetcher] = {}

def register_library_fetcher(fetcher: LibraryFetcher) -> None:
    """Register a provider-specific library fetcher instance."""
    _REGISTRY[fetcher.scheme] = fetcher

def get_library_fetcher(scheme: str) -> LibraryFetcher | None:
    """Retrieve the registered library fetcher for a scheme."""
    return _REGISTRY.get(scheme)
