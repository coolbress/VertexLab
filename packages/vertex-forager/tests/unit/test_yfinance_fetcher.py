from types import SimpleNamespace

import pytest

from vertex_forager.core.config import RequestSpec
from vertex_forager.providers.yfinance.fetcher import fetch_yfinance


def test_fetch_yfinance_download() -> None:
    fake_yf = SimpleNamespace(download=lambda tickers, **kwargs: {"tickers": tickers, **kwargs})

    spec = RequestSpec(
        url="yfinance://AAPL",
        params={
            "dataset": "price",
            "lib": {
                "type": "download",
                "kwargs": {"period": "1d"},
            },
        },
    )

    assert fetch_yfinance(spec, yf_lib=fake_yf) == {"tickers": "AAPL", "period": "1d"}


def test_fetch_yfinance_requires_optional_dependency() -> None:
    spec = RequestSpec(
        url="yfinance://AAPL",
        params={
            "dataset": "price",
            "lib": {
                "type": "download",
            },
        },
    )

    with pytest.raises(ImportError, match="Install with: pip install vertex-forager\\[yfinance\\]"):
        fetch_yfinance(spec, yf_lib=None)


def test_fetch_yfinance_validates_missing_lib_spec() -> None:
    spec = RequestSpec(url="yfinance://AAPL", params={"dataset": "price"})

    with pytest.raises(ValueError, match="Missing library call specification"):
        fetch_yfinance(spec, yf_lib=SimpleNamespace(download=lambda **_: None))


def test_fetch_yfinance_rejects_empty_payload() -> None:
    spec = RequestSpec(
        url="yfinance://   ",
        params={
            "dataset": "price",
            "lib": {
                "type": "download",
            },
        },
    )

    with pytest.raises(ValueError, match="Empty library payload"):
        fetch_yfinance(spec, yf_lib=SimpleNamespace(download=lambda **_: None))


def test_fetch_yfinance_rejects_invalid_attr() -> None:
    fake_yf = SimpleNamespace(Ticker=lambda _ticker: SimpleNamespace(history=lambda **_: None))

    spec = RequestSpec(
        url="yfinance://AAPL",
        params={
            "dataset": "price",
            "lib": {
                "type": "ticker_attr",
                "attr": "_secret",
            },
        },
    )

    with pytest.raises(ValueError, match="Unknown yfinance dataset"):
        fetch_yfinance(spec, yf_lib=fake_yf)


def test_fetch_yfinance_rejects_invalid_kwargs_type() -> None:
    spec = RequestSpec(
        url="yfinance://AAPL",
        params={
            "dataset": "price",
            "lib": {
                "type": "download",
                "kwargs": ["1d"],
            },
        },
    )

    with pytest.raises(ValueError, match="Invalid library kwargs"):
        fetch_yfinance(spec, yf_lib=SimpleNamespace(download=lambda **_: None))


def test_fetch_yfinance_allows_only_dataset_mapped_attr() -> None:
    fake_yf = SimpleNamespace(Ticker=lambda _ticker: SimpleNamespace(history=lambda **kwargs: kwargs))

    spec = RequestSpec(
        url="yfinance://AAPL",
        params={
            "dataset": "price",
            "lib": {
                "type": "ticker_attr",
                "attr": "history",
                "kwargs": {"period": "1mo"},
            },
        },
    )

    assert fetch_yfinance(spec, yf_lib=fake_yf) == {"period": "1mo"}


def test_fetch_yfinance_rejects_kwargs_for_non_callable_attr() -> None:
    fake_yf = SimpleNamespace(Ticker=lambda _ticker: SimpleNamespace(info={"symbol": "AAPL"}))

    spec = RequestSpec(
        url="yfinance://AAPL",
        params={
            "dataset": "info",
            "lib": {
                "type": "ticker_attr",
                "attr": "info",
                "kwargs": {"period": "1mo"},
            },
        },
    )

    with pytest.raises(ValueError, match="non-callable yfinance dataset"):
        fetch_yfinance(spec, yf_lib=fake_yf)
