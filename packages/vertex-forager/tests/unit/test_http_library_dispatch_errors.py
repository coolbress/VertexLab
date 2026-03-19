import pytest
from vertex_forager.core.config import RequestSpec
from vertex_forager.core.http import HttpExecutor


@pytest.mark.asyncio
async def test_library_dispatch_unsupported_scheme_raises_valueerror() -> None:
    class Client:
        async def run_sync(self, fn):
            return fn()
    ex = HttpExecutor(client=Client())
    spec = RequestSpec(url="nosuch://AAPL", params={"dataset": "price"})
    with pytest.raises(ValueError, match=r".*"):
        await ex.fetch(spec)
