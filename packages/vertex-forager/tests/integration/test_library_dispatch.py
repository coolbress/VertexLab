import pytest
from vertex_forager.core.config import RequestSpec
from vertex_forager.core.http import HttpExecutor
import json


class DummyYF:
    class Ticker:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol
        def info(self) -> dict[str, str]:
            return {"symbol": self.symbol, "ok": "1"}
    def download(self, tickers: str, **kwargs):
        return {"tickers": tickers, "kwargs": kwargs}


@pytest.mark.asyncio
async def test_core_http_dispatch_library(monkeypatch):
    import vertex_forager.core.http as http_mod
    import vertex_forager.providers.yfinance.fetcher as _yf_fetcher  # noqa: F401
    monkeypatch.setattr(http_mod, "yf", DummyYF())
    spec = RequestSpec(
        url="yfinance://AAPL",
        params={"dataset": "price", "lib": {"type": "ticker_attr", "attr": "info", "kwargs": {}}},
    )
    class Client:
        async def run_sync(self, fn):
            return fn()
    ex = HttpExecutor(client=Client())
    payload = await ex.fetch(spec)
    obj = json.loads(payload.decode("utf-8"))
    assert isinstance(obj, dict)
    assert obj.get("ok") == 1 or obj.get("ok") == "1"
