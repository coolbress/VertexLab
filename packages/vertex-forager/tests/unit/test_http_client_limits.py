import pytest
from vertex_forager.core.http import default_async_client


@pytest.mark.asyncio
async def test_default_async_client_uses_env_timeouts(monkeypatch) -> None:
    monkeypatch.setenv("VF_HTTP_TIMEOUT_S", "7.5")
    async with default_async_client() as client:
        # httpx exposes timeout on client
        # The Timeout has .connect attribute; it may be float or object; compare total/values as strings
        to = client.timeout
        # Values may be float or None if unset; when set via default_async_client, they should equal 7.5
        for attr in ("connect", "read", "write", "pool"):
            val = getattr(to, attr)
            assert val is not None
            assert abs(float(val) - 7.5) < 1e-6
