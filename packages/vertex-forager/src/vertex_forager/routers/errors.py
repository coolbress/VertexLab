"""Provider-agnostic router error adapters.

Standardizes provider-specific error payloads/exceptions into Vertex Forager
common exception types for consistent handling in router implementations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NoReturn

import httpx

from vertex_forager.exceptions import FetchError, TransformError

if TYPE_CHECKING:
    from collections.abc import Mapping


def raise_quandl_error(provider: str, err: Mapping[str, Any]) -> NoReturn:
    """Raise a standardized FetchError for Quandl-style API errors.

    Args:
        provider: Provider identifier (e.g., 'sharadar').
        err: Error payload containing optional 'code' and 'message'.

    Raises:
        FetchError: Standardized fetch error with provider context.
    """
    code = err.get("code", "Unknown")
    message = err.get("message", "Unknown error")
    raise FetchError(f"{provider} API error {code}: {message}")


def raise_yfinance_parse_error(exc: Exception, *, dataset: str) -> NoReturn:
    """Raise standardized TransformError for yfinance parse failures.

    Preserves UnpicklingError to satisfy tests expecting the original exception.
    """
    if exc.__class__.__name__ == "UnpicklingError" and exc.__class__.__module__ in {"pickle", "_pickle"}:
        raise exc
    if isinstance(exc, httpx.HTTPError):
        raise FetchError(f"yfinance HTTP error for '{dataset}': {exc}")
    raise TransformError(f"yfinance parse error for '{dataset}': {exc}")
