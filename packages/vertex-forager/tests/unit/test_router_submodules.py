import polars as pl
from datetime import datetime, timezone
import pytest

from vertex_forager.routers.transforms import (
    add_provider_metadata,
    check_empty_response,
    parse_date_range,
    normalize_columns,
)
from vertex_forager.routers.jobs import single_symbol_job, pagination_job
from vertex_forager.routers.errors import raise_quandl_error
from vertex_forager.core.config import RequestAuth
from vertex_forager.exceptions import FetchError


class TestTransforms:
    def test_normalize_columns_lower_snake(self):
        df = pl.DataFrame({"Ticker Symbol": [1], "Price-USD": [2]})
        out = normalize_columns(df)
        assert out.columns == ["ticker_symbol", "price_usd"]

    def test_check_empty_response_on_empty_frame(self):
        df = pl.DataFrame()
        res = check_empty_response(frame=df)
        assert res is not None and res.packets == []

    def test_add_provider_metadata(self):
        df = pl.DataFrame({"a": [1]})
        out = add_provider_metadata("yfinance", frame=df, observed_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        assert "provider" in out.columns and "fetched_at" in out.columns

    def test_parse_date_range_valid(self):
        start_end = parse_date_range("2024-01-01", "2024-01-31")
        assert start_end is not None and start_end[0].year == 2024


class TestJobs:
    def test_single_symbol_job_without_auth(self):
        job = single_symbol_job(
            provider="yfinance",
            dataset="price",
            symbol="AAPL",
            url="yfinance://AAPL",
            params={"dataset": "price"},
        )
        assert job.symbol == "AAPL" and job.provider == "yfinance"

    def test_pagination_job_with_auth(self):
        auth = RequestAuth(kind="none")
        job = pagination_job(
            provider="sharadar",
            dataset="tickers",
            url="https://api.example/tickers",
            params={"qopts.per_page": "10000"},
            auth=auth,
            context={"pagination": {"meta_key": "next_cursor", "cursor_param": "cursor"}},
        )
        assert job.context.get("pagination") is not None


class TestErrors:
    def test_raise_quandl_error(self):
        with pytest.raises(FetchError):
            raise_quandl_error("sharadar", {"code": 400, "message": "bad request"})
