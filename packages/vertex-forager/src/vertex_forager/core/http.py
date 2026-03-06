from __future__ import annotations

import logging
from typing import Any
import re
import httpx
import io
import json
import polars as pl
import pandas as pd
try:
    import yfinance as yf  # test compatibility: allow monkeypatching core.http.yf
except ImportError:
    yf = None
from vertex_forager.core.types import JSONValue
from vertex_forager.core.config import RequestSpec
from vertex_forager.constants import (
    HTTP_TIMEOUT_S,
    HTTP_MAX_KEEPALIVE_CONNECTIONS,
    HTTP_MAX_CONNECTIONS,
    HTTP_USER_AGENT,
)
from vertex_forager.core.library import get_library_fetcher
from vertex_forager.utils import env_int, env_float


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
    
    Notes:
        - Error logs redact URLs using a sanitizer to avoid leaking sensitive query
          parameters (e.g., API keys) in messages.
        - Example: "https://api.example.com?token=SECRET&cursor=123" -> "[redacted]"
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
        
        Args:
            spec: Request specification including method, url, headers, params, body, and timeout.
        
        Returns:
            bytes: Raw response content.
        
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
            resp: httpx.Response = await self._client.run_async(
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
        
        Args:
            spec: Request specification with a library URL scheme (e.g., yfinance://).
        
        Returns:
            bytes: Serialized payload (IPC for DataFrame-like, JSON for others).
        
        Raises:
            ValueError: Unsupported scheme or invalid library call configuration.
            TypeError: Invalid types passed to library call.
        """
        scheme = spec.url.split("://", 1)[0]
        params = spec.params
        dataset = params.get("dataset", "price")

        try:
            # 1. Execute provider-specific library call via registry
            fetcher = get_library_fetcher(scheme)
            if fetcher is None:
                raise ValueError(f"Unsupported library scheme: {scheme}")

            def _execute() -> Any:
                return fetcher.fetch(spec)

            # Use the client's run_sync method
            data = await self._client.run_sync(_execute)
            # Serialize payload: IPC for DataFrame-like, JSON for others
            if isinstance(data, pl.DataFrame):
                buf = io.BytesIO()
                data.write_ipc(buf)
                return buf.getvalue()
            if isinstance(data, (pd.DataFrame, pd.Series)):
                if isinstance(data, pd.Series):
                    df_pd = data.to_frame().reset_index()
                else:
                    df_pd = data.reset_index()
                df_pl = pl.from_pandas(df_pd)
                buf = io.BytesIO()
                df_pl.write_ipc(buf)
                return buf.getvalue()
            try:
                return json.dumps(data).encode("utf-8")
            except (TypeError, ValueError):
                return json.dumps(data, default=str).encode("utf-8")

        except (ValueError, TypeError) as e:
            prov = self._client.__class__.__name__
            msg = _redact_urls(str(e))
            logger.error(
                "Library fetch failed provider=%s scheme=%s dataset=%s symbol=%s exc=%s msg=%s",
                prov,
                scheme,
                dataset,
                spec.url.split("://", 1)[1],
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
    
    Returns:
        httpx.AsyncClient: Configured client for HTTP operations.
    """
    
    mk = env_int("VF_HTTP_MAX_KEEPALIVE", HTTP_MAX_KEEPALIVE_CONNECTIONS)
    max_keepalive = mk if mk is not None and mk > 0 else HTTP_MAX_KEEPALIVE_CONNECTIONS

    mc = env_int("VF_HTTP_MAX_CONNECTIONS", HTTP_MAX_CONNECTIONS)
    max_conns = mc if mc is not None and mc > 0 else HTTP_MAX_CONNECTIONS

    to = env_float("VF_HTTP_TIMEOUT_S", HTTP_TIMEOUT_S)
    timeout_s = to if to is not None and to > 0 else HTTP_TIMEOUT_S
    return httpx.AsyncClient(
        headers={"User-Agent": HTTP_USER_AGENT},
        timeout=httpx.Timeout(timeout_s),
        limits=httpx.Limits(
            max_keepalive_connections=max_keepalive,
            max_connections=max_conns,
        ),
    )
