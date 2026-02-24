from __future__ import annotations

import pickle
import logging
from typing import Any
import httpx
import yfinance as yf
from vertex_forager.core.config import RequestSpec


logger = logging.getLogger("vertex_forager.core.http")


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
        """
        # Dispatch based on scheme
        if "://" in spec.url and not spec.url.startswith(("http://", "https://")):
            return await self._fetch_library(spec)
        return await self._fetch_http(spec)

    async def _fetch_http(self, spec: RequestSpec) -> bytes:
        """Execute a standard HTTP request using the unified client interface."""
        headers = dict(spec.headers)
        params = dict(spec.params)

        if spec.auth.kind == "bearer" and spec.auth.token:
            headers["Authorization"] = f"Bearer {spec.auth.token}"
        elif spec.auth.kind == "header" and spec.auth.token and spec.auth.header_name:
            headers[spec.auth.header_name] = spec.auth.token
        elif spec.auth.kind == "query" and spec.auth.token and spec.auth.query_param:
            params[spec.auth.query_param] = spec.auth.token

        # Use run_async from BaseClient interface
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

    async def _fetch_library(self, spec: RequestSpec) -> bytes:
        """Execute a non-HTTP library call using the unified client interface."""
        scheme, payload = spec.url.split("://", 1)
        params = spec.params
        dataset = params.get("dataset", "price")

        try:
            # 1. Execute provider-specific library call
            def _execute():
                if scheme != "yfinance":
                    raise ValueError(f"Unsupported library scheme: {scheme}")
                ticker_symbol = payload
                endpoint = params.get("endpoint")
                if endpoint == "download":
                    download_kwargs = {**params}
                    download_kwargs.pop("dataset", None)
                    download_kwargs.pop("endpoint", None)
                    return yf.download(tickers=ticker_symbol, **download_kwargs)
                ticker = yf.Ticker(ticker_symbol)
                if not hasattr(ticker, dataset):
                    raise ValueError(f"Unknown yfinance dataset: {dataset}")
                return getattr(ticker, dataset)

            # Use the client's run_sync method
            data = await self._client.run_sync(_execute)
            
            # Use pickle to preserve the exact Python object structure (Raw Data)
            # This allows the Router to handle normalization and schema mapping properly.
            return pickle.dumps(data)

        except (ValueError, TypeError) as e:
            logger.error(f"Library fetch failed for scheme={scheme}, dataset={dataset}: {e}")
            raise


def default_async_client() -> httpx.AsyncClient:
    """Create a default httpx AsyncClient instance.

    Configured with reasonable defaults for high-concurrency scraping:
    - User-Agent: vertex-forager
    - Timeout: 60 seconds (increased for large datasets)
    - Connection Pool: 200 max connections, 100 keep-alive (reduced handshake overhead)
    """
    return httpx.AsyncClient(
        headers={"User-Agent": "vertex-forager"},
        timeout=httpx.Timeout(60.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=200),
    )
