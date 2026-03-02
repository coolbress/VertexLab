from __future__ import annotations

import pickle
import logging
from typing import Any
import re
import httpx
import yfinance as yf
from vertex_forager.core.types import JSONValue
from vertex_forager.core.config import RequestSpec
from vertex_forager.constants import (
    HTTP_TIMEOUT_S,
    HTTP_MAX_KEEPALIVE_CONNECTIONS,
    HTTP_MAX_CONNECTIONS,
    HTTP_USER_AGENT,
)


logger = logging.getLogger("vertex_forager.core.http")

_URL_REDACT_RE = re.compile(r"https?://\S+")

def _redact_urls(message: str) -> str:
    return _URL_REDACT_RE.sub("[redacted]", message)


class HttpExecutor:
    """Async HTTP Request Executor using httpx.

    This class abstracts the low-level HTTP client details and maps `RequestSpec`
    objects to actual network requests. It handles authentication header injection
    and response status checking.

    It also supports special schemes like `yfinance://` to bypass HTTP and use internal libraries.
    """

    def __init__(self, *, client: Any) -> None:
        """Initialize with an existing client.

        Args:
            client: `BaseClient` (or compatible interface) to use for requests.
        """
        self._client = client

    async def fetch(self, spec: RequestSpec) -> bytes:
        """Execute a request and return response bytes.

        Dispatches to specific fetch implementation based on URL scheme.

        Args:
            spec: Fully defined request specification.

        Returns:
            bytes: The raw response body.

        Raises:
            httpx.HTTPStatusError: If the server returns 4xx/5xx status code.
            httpx.RequestError: If a network error occurs.
            ValueError: If URL scheme is invalid.
            TypeError: If library request parameters are invalid.
        """
        # Dispatch based on scheme
        if "://" in spec.url and not spec.url.startswith(("http://", "https://")):
            return await self._fetch_library(spec)
        return await self._fetch_http(spec)

    async def _fetch_http(self, spec: RequestSpec) -> bytes:
        """Execute a standard HTTP request using the unified client interface.

        Raises:
            httpx.RequestError: Network error during HTTP request.
            httpx.HTTPStatusError: Non-2xx HTTP status returned.
            TypeError: When parameters are invalid for the underlying client.
        """
        headers: dict[str, str] = dict(spec.headers)
        params: dict[str, "JSONValue"] = dict(spec.params)

        if spec.auth.kind == "bearer" and spec.auth.token:
            headers["Authorization"] = f"Bearer {spec.auth.token}"
        elif spec.auth.kind == "header" and spec.auth.token and spec.auth.header_name:
            headers[spec.auth.header_name] = spec.auth.token
        elif spec.auth.kind == "query" and spec.auth.token and spec.auth.query_param:
            params[spec.auth.query_param] = spec.auth.token

        try:
            resp = await self._client.run_async(
                spec.method.value,
                spec.url,
                params=params,
                headers=headers,
                json=spec.json_body,
                content=spec.data,
                timeout=spec.timeout_s,
            )
            resp.raise_for_status()
            return resp.content
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            prov = self._client.__class__.__name__
            status = getattr(getattr(e, "response", None), "status_code", None)
            msg = _redact_urls(str(e))
            logger.error("HTTP fetch failed provider=%s status=%s exc=%s msg=%s", prov, status, type(e).__name__, msg)
            raise

    async def _fetch_library(self, spec: RequestSpec) -> bytes:
        """Execute a non-HTTP library call using the unified client interface.

        Raises:
            ValueError: Unsupported scheme or invalid library call configuration.
            TypeError: Invalid types passed to library call.
        """
        scheme, payload = spec.url.split("://", 1)
        params = spec.params
        dataset = params.get("dataset", "price")
        lib = params.get("lib")

        try:
            # 1. Execute provider-specific library call
            def _execute():
                if scheme != "yfinance":
                    raise ValueError(f"Unsupported library scheme: {scheme}")
                ticker_symbol = payload
                if not isinstance(lib, dict):
                    raise ValueError("Missing library call specification ('lib') in request params")
                call_type = lib.get("type")
                kw = lib.get("kwargs")
                call_kwargs: dict[str, "JSONValue"] = dict(kw) if isinstance(kw, dict) else {}
                if call_type == "download":
                    return yf.download(tickers=ticker_symbol, **call_kwargs)
                if call_type == "ticker_attr":
                    attr_name = lib.get("attr")
                    ticker = yf.Ticker(ticker_symbol)
                    if not isinstance(attr_name, str) or not hasattr(ticker, attr_name):
                        raise ValueError(f"Unknown yfinance dataset: {dataset} -> {attr_name}")
                    attr = getattr(ticker, attr_name)
                    return attr(**call_kwargs) if callable(attr) else attr
                raise ValueError(f"Unsupported library call type: {call_type}")

            # Use the client's run_sync method
            data = await self._client.run_sync(_execute)
            
            # Use pickle to preserve the exact Python object structure (Raw Data)
            # This allows the Router to handle normalization and schema mapping properly.
            return pickle.dumps(data)

        except (ValueError, TypeError) as e:
            prov = self._client.__class__.__name__
            msg = _redact_urls(str(e))
            logger.error(
                "Library fetch failed provider=%s scheme=%s dataset=%s symbol=%s exc=%s msg=%s",
                prov,
                scheme,
                dataset,
                payload,
                type(e).__name__,
                msg,
            )
            raise


def default_async_client() -> httpx.AsyncClient:
    """Create a default httpx AsyncClient instance.

    Configured with centralized defaults (see vertex_forager.constants):
    - User-Agent: HTTP_USER_AGENT
    - Timeout: HTTP_TIMEOUT_S seconds
    - Connection Pool: HTTP_MAX_CONNECTIONS (max), HTTP_MAX_KEEPALIVE_CONNECTIONS (keep-alive)
    """
    return httpx.AsyncClient(
        headers={"User-Agent": HTTP_USER_AGENT},
        timeout=httpx.Timeout(HTTP_TIMEOUT_S),
        limits=httpx.Limits(
            max_keepalive_connections=HTTP_MAX_KEEPALIVE_CONNECTIONS,
            max_connections=HTTP_MAX_CONNECTIONS,
        ),
    )
