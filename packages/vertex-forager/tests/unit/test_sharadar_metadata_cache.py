from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from vertex_forager.providers.sharadar.client import SharadarClient


def _metadata_df(*, tickers: list[str], fetched_at: datetime) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": tickers,
            "name": [f"{ticker} Inc." for ticker in tickers],
            "firstpricedate": ["2020-01-01"] * len(tickers),
            "lastpricedate": ["2024-01-01"] * len(tickers),
            "fetched_at": [fetched_at] * len(tickers),
        }
    )


@pytest.fixture
def client() -> SharadarClient:
    return SharadarClient(api_key="test_api_key", rate_limit=100)  # pragma: allowlist secret (test)


@pytest.mark.asyncio
async def test_get_ticker_info_uses_full_disk_cache_for_all_tickers(
    client: SharadarClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("vertex_forager.providers.sharadar.client.get_cache_dir", lambda: tmp_path)
    fresh = _metadata_df(
        tickers=["AAPL", "MSFT"],
        fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    client._save_metadata_to_disk(fresh)

    async def _fail_fetch(*args: object, **kwargs: object) -> pl.DataFrame:
        raise AssertionError("full metadata fetch should not run on fresh disk cache")

    monkeypatch.setattr(client, "_fetch_pagination", _fail_fetch, raising=True)

    result = await client._get_ticker_info_async(tickers=None, connect_db=None)

    assert isinstance(result, pl.DataFrame)
    assert set(result.get_column("ticker").to_list()) == {"AAPL", "MSFT"}


@pytest.mark.asyncio
async def test_get_ticker_info_uses_disk_cache_even_with_connect_db(
    client: SharadarClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("vertex_forager.providers.sharadar.client.get_cache_dir", lambda: tmp_path)
    fresh = _metadata_df(
        tickers=["AAPL"],
        fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    client._save_metadata_to_disk(fresh)

    async def _fail_fetch(*args: object, **kwargs: object) -> pl.DataFrame:
        raise AssertionError("metadata fetch should not depend on connect_db")

    monkeypatch.setattr(client, "_fetch_per_ticker", _fail_fetch, raising=True)

    result = await client._get_ticker_info_async(tickers=["AAPL"], connect_db="duckdb:///tmp/demo.duckdb")

    assert isinstance(result, pl.DataFrame)
    assert result.get_column("ticker").to_list() == ["AAPL"]


@pytest.mark.asyncio
async def test_get_ticker_info_fetches_only_expired_tickers_from_disk_cache(
    client: SharadarClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = _metadata_df(
        tickers=["AAPL"],
        fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    seen: dict[str, list[str] | None] = {}

    async def _fake_fetch(cfg):  # type: ignore[no-untyped-def]
        seen["symbols"] = cfg.symbols
        return _metadata_df(
            tickers=["MSFT"],
            fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )

    monkeypatch.setattr(client, "_load_metadata_from_disk", lambda tickers: None, raising=True)
    monkeypatch.setattr(client, "_load_metadata_parts_from_disk", lambda tickers: (fresh, ["MSFT"]), raising=True)
    monkeypatch.setattr(client, "_fetch_per_ticker", _fake_fetch, raising=True)

    result = await client._get_ticker_info_async(tickers=["AAPL", "MSFT"], connect_db=None)

    assert seen["symbols"] == ["MSFT"]
    assert isinstance(result, pl.DataFrame)
    assert set(result.get_column("ticker").to_list()) == {"AAPL", "MSFT"}


@pytest.mark.asyncio
async def test_get_ticker_info_fetches_only_missing_tickers_from_disk_cache(
    client: SharadarClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = _metadata_df(
        tickers=["AAPL"],
        fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    seen: dict[str, list[str] | None] = {}

    async def _fake_fetch(cfg):  # type: ignore[no-untyped-def]
        seen["symbols"] = cfg.symbols
        return _metadata_df(
            tickers=["MSFT"],
            fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )

    monkeypatch.setattr(client, "_load_metadata_from_disk", lambda tickers: None, raising=True)
    monkeypatch.setattr(client, "_fetch_per_ticker", _fake_fetch, raising=True)
    monkeypatch.setattr(client, "_load_metadata_parts_from_disk", lambda tickers: (fresh, ["MSFT"]), raising=True)

    result = await client._get_ticker_info_async(tickers=["AAPL", "MSFT"], connect_db=None)

    assert seen["symbols"] == ["MSFT"]
    assert isinstance(result, pl.DataFrame)
    assert set(result.get_column("ticker").to_list()) == {"AAPL", "MSFT"}


def test_load_metadata_from_disk_returns_none_when_any_row_is_expired(
    client: SharadarClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("vertex_forager.providers.sharadar.client.get_cache_dir", lambda: tmp_path)
    stale = _metadata_df(
        tickers=["AAPL"],
        fetched_at=datetime.now(timezone.utc) - timedelta(days=2),
    )
    client._save_metadata_to_disk(stale)

    assert client._load_metadata_from_disk(["AAPL"]) is None


def test_save_metadata_to_disk_upserts_latest_row(
    client: SharadarClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("vertex_forager.providers.sharadar.client.get_cache_dir", lambda: tmp_path)
    old = _metadata_df(
        tickers=["AAPL"],
        fetched_at=datetime.now(timezone.utc) - timedelta(days=3),
    )
    new = _metadata_df(
        tickers=["AAPL", "MSFT"],
        fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )

    client._save_metadata_to_disk(old)
    client._save_metadata_to_disk(new)

    cached = client._load_metadata_from_disk(["AAPL", "MSFT"])

    assert cached is not None
    assert cached.height == 2
    assert set(cached.get_column("ticker").to_list()) == {"AAPL", "MSFT"}
