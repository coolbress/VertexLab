import pytest

from vertex_forager.constants import HTTP_TIMEOUT_S
from vertex_forager.core.http import default_async_client


@pytest.mark.asyncio
async def test_default_async_client_uses_library_defaults() -> None:
    async with default_async_client() as client:
        # httpx exposes timeout on client
        # Timeout has .connect; it may be float or object
        # Compare total/values as strings
        to = client.timeout
        # Values may be float or None; when set by default_async_client, expect library defaults.
        for attr in ("connect", "read", "write", "pool"):
            val = getattr(to, attr)
            assert val is not None
            assert abs(float(val) - HTTP_TIMEOUT_S) < 1e-6
