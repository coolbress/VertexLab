from __future__ import annotations

import logging

import httpx
from vertex_forager.core.config import RequestSpec


logger = logging.getLogger("vertex_forager.core.http")


class HttpExecutor:
    """Async HTTP Request Executor using httpx.

    This class abstracts the low-level HTTP client details and maps `RequestSpec`
    objects to actual network requests. It handles authentication header injection
    and response status checking.
    """

    def __init__(self, *, client: httpx.AsyncClient) -> None:
        """Initialize with an existing httpx client.

        Args:
            client: Shared `httpx.AsyncClient` instance to use for requests.
        """
        self._client = client

    async def fetch(self, spec: RequestSpec) -> bytes:
        """Execute a request and return response bytes.

        Processes the `RequestSpec` to inject authentication (Bearer, Header, or Query)
        and executes the HTTP method (GET, POST, etc.) with the configured timeout.

        Args:
            spec: Fully defined request specification.

        Returns:
            bytes: The raw response body.

        Raises:
            httpx.HTTPStatusError: If the server returns 4xx/5xx status code.
            httpx.RequestError: If a network error occurs.
        """
        headers = dict(spec.headers)
        params = dict(spec.params)

        if spec.auth.kind == "bearer" and spec.auth.token:
            headers["Authorization"] = f"Bearer {spec.auth.token}"
        elif spec.auth.kind == "header" and spec.auth.token and spec.auth.header_name:
            headers[spec.auth.header_name] = spec.auth.token
        elif spec.auth.kind == "query" and spec.auth.token and spec.auth.query_param:
            params[spec.auth.query_param] = spec.auth.token

        resp = await self._client.request(
            spec.method.value,
            spec.url,
            params=params,
            headers=headers,
            json=spec.json_body,
            content=spec.data,
            timeout=spec.timeout_s,
        )
        
        # logger.debug(f"HTTP Request latency: {time.monotonic() - t_start:.3f}s | URL: {spec.url}")

        resp.raise_for_status()
        return resp.content


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
