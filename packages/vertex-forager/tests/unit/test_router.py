"""
Unit tests for router functionality.

Summary:
    Tests for SharadarRouter class, covering job generation, parsing, and edge cases.

Notes:
    - Mocking is used to isolate router logic from network calls.
    - Interface contracts are verified against FetchJob and RequestSpec definitions.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from vertex_forager.core.config import FetchJob, ParseResult, RequestSpec
from vertex_forager.providers.sharadar.router import SharadarRouter


class TestSharadarRouterUnit:
    """Unit tests for SharadarRouter functionality."""

    @pytest.fixture
    def router(self) -> SharadarRouter:
        """Create a SharadarRouter instance for testing."""
        return SharadarRouter(
            api_key="test_api_key",
            rate_limit=500,
        )

    def test_router_provider_property_returns_correct_value(
        self, router: SharadarRouter
    ) -> None:
        """Test that provider property returns expected value."""
        # Act & Assert
        assert router.provider == "sharadar"

    @pytest.mark.asyncio
    async def test_generate_jobs_creates_correct_fetch_jobs_for_price_dataset(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs creates proper FetchJob instances for price dataset."""
        # Default behavior is 1 job per symbol (no implicit batching)
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="price", symbols=["AAPL", "MSFT"]
            )
        ]

        assert len(jobs) == 2
        job = jobs[0]
        assert job.provider == "sharadar"
        assert job.dataset == "price"
        assert isinstance(job.spec, RequestSpec)
        assert job.spec.params.get("ticker") in ["AAPL", "MSFT"]

    @pytest.mark.asyncio
    async def test_generate_jobs_handles_empty_symbols_list(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs handles empty symbols list gracefully."""
        # Act
        jobs = [job async for job in router.generate_jobs(dataset="price", symbols=[])]

        # Assert
        assert len(jobs) == 0

    @pytest.mark.asyncio
    async def test_generate_jobs_handles_none_symbols(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs handles None symbols gracefully."""
        # Act
        jobs = [
            job async for job in router.generate_jobs(dataset="price", symbols=None)
        ]

        # Assert
        assert len(jobs) == 0

    @pytest.mark.asyncio
    async def test_generate_jobs_passes_kwargs_for_tickers_dataset(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs passes kwargs correctly for tickers dataset."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="tickers", symbols=None, per_page=500
            )
        ]

        assert len(jobs) == 1
        job = jobs[0]
        assert job.dataset == "tickers"
        assert job.spec.params.get("qopts.per_page") == "500"

    @pytest.mark.asyncio
    async def test_generate_jobs_passes_kwargs_for_fundamental_dataset(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs passes kwargs correctly for fundamental dataset."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="fundamental", symbols=["AAPL"], dimension="ARQ"
            )
        ]

        assert len(jobs) == 1
        job = jobs[0]
        assert job.dataset == "fundamental"
        assert job.spec.params.get("dimension") == "ARQ"

    @pytest.mark.asyncio
    async def test_generate_jobs_does_not_batch_by_default(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs does not batch symbols by default (safety first)."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="price",
                symbols=["AAPL", "MSFT", "TSLA"],
            )
        ]

        assert len(jobs) == 3

    @pytest.mark.asyncio
    async def test_generate_jobs_ignores_bulk_size(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs ignores bulk_size and maintains 1-to-1 mapping."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="price", symbols=["AAPL", "MSFT", "TSLA"], bulk_size=3
            )
        ]

        # Expect 3 jobs because Router no longer chunks
        assert len(jobs) == 3

    @pytest.mark.asyncio
    async def test_generate_jobs_handles_pre_batched_strings(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs handles comma-separated strings as single jobs."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="price",
                symbols=["AAPL,MSFT,TSLA"],
            )
        ]

        assert len(jobs) == 1
        assert jobs[0].spec.params.get("ticker") == "AAPL,MSFT,TSLA"

    @pytest.mark.asyncio
    async def test_generate_jobs_creates_correct_fetch_jobs_for_actions_dataset(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs creates proper FetchJob instances for actions dataset."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="actions", symbols=["AAPL", "MSFT"]
            )
        ]

        assert len(jobs) == 2
        job = jobs[0]
        assert job.provider == "sharadar"
        assert job.dataset == "actions"
        assert job.spec.url.endswith("ACTIONS.json")
        assert job.spec.params.get("ticker") in ["AAPL", "MSFT"]

    @pytest.mark.asyncio
    async def test_generate_jobs_creates_correct_fetch_jobs_for_daily_dataset(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs creates proper FetchJob instances for daily dataset."""
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="daily", symbols=["AAPL", "MSFT"]
            )
        ]

        assert len(jobs) == 2
        job = jobs[0]
        assert job.provider == "sharadar"
        assert job.dataset == "daily"
        assert job.spec.url.endswith("DAILY.json")
        assert job.spec.params.get("ticker") in ["AAPL", "MSFT"]

    @pytest.mark.asyncio
    async def test_generate_jobs_creates_correct_fetch_jobs_for_sp500_dataset(
        self, router: SharadarRouter
    ) -> None:
        """Test that generate_jobs creates proper FetchJob instances for sp500 dataset."""
        # SP500 dataset might be fetched for specific tickers or all
        jobs = [
            job
            async for job in router.generate_jobs(
                dataset="sp500", symbols=["AAPL", "MSFT"]
            )
        ]

        assert len(jobs) == 2
        job = jobs[0]
        assert job.provider == "sharadar"
        assert job.dataset == "sp500"
        assert job.spec.url.endswith("SP500.json")
        assert job.spec.params.get("ticker") in ["AAPL", "MSFT"]

    def test_parse_method_converts_json_to_dataframe(
        self, router: SharadarRouter
    ) -> None:
        """Test that parse method correctly converts JSON payload to DataFrame."""
        # Arrange
        job = FetchJob(
            provider="sharadar",
            dataset="price",
            symbol="AAPL",
            spec=RequestSpec(url="https://api.sharadar.com/SEP.json"),
        )

        payload = {
            "datatable": {
                "data": [
                    [
                        "AAPL",
                        "2024-01-02",
                        "100.0",
                        "101.0",
                        "99.0",
                        "100.5",
                        "1000000",
                        "100.5",
                        "100.0",
                    ]
                ],
                "columns": [
                    {"name": "ticker"},
                    {"name": "date"},
                    {"name": "open"},
                    {"name": "high"},
                    {"name": "low"},
                    {"name": "close"},
                    {"name": "volume"},
                    {"name": "closeadj"},
                    {"name": "closeunadj"},
                ],
            }
        }

        # Act
        result = router.parse(job=job, payload=json.dumps(payload).encode("utf-8"))

        # Assert
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 1

        packet = result.packets[0]
        assert packet.provider == "sharadar"
        assert packet.table == "sharadar_sep"
        assert isinstance(packet.frame, pl.DataFrame)
        assert packet.frame.height == 1
        assert "ticker" in packet.frame.columns
        assert "open" in packet.frame.columns

    def test_parse_method_handles_empty_data(self, router: SharadarRouter) -> None:
        """Test that parse method handles empty data gracefully."""
        # Arrange
        job = FetchJob(
            provider="sharadar",
            dataset="price",
            symbol="AAPL",
            spec=RequestSpec(url="https://api.sharadar.com/SEP.json"),
        )

        payload = {"datatable": {"data": [], "columns": []}}

        # Act
        result = router.parse(job=job, payload=json.dumps(payload).encode("utf-8"))

        # Assert
        assert isinstance(result, ParseResult)
        assert len(result.packets) == 0

    def test_parse_creates_next_jobs_when_pagination_context_exists(
        self, router: SharadarRouter
    ) -> None:
        """Test that parse method creates next_jobs when pagination context exists and next_cursor is returned."""
        # Arrange
        context = {
            "pagination": {
                "cursor_param": "qopts.cursor_id",
                "meta_key": "next_cursor_id",
                "max_pages": 1000,
            }
        }
        job = FetchJob(
            provider="sharadar",
            dataset="price",
            symbol="AAPL,MSFT",
            spec=RequestSpec(
                url="https://api.sharadar.com/SEP.json", params={"ticker": "AAPL,MSFT"}
            ),
            context=context,
        )

        payload = {
            "datatable": {
                "data": [["AAPL", "2024-01-02", 100.0]],
                "columns": [{"name": "ticker"}, {"name": "date"}, {"name": "close"}],
            },
            "meta": {"next_cursor_id": "cursor_12345"},
        }

        # Act
        result = router.parse(job=job, payload=json.dumps(payload).encode("utf-8"))

        # Assert
        assert isinstance(result, ParseResult)
        assert len(result.next_jobs) == 1

        next_job = result.next_jobs[0]
        assert next_job.dataset == "price"
        assert next_job.spec.params.get("qopts.cursor_id") == "cursor_12345"
        # Ensure other params are preserved
        assert next_job.spec.params.get("ticker") == "AAPL,MSFT"

    def test_parse_method_handles_malformed_json(self, router: SharadarRouter) -> None:
        """Test that parse method handles malformed JSON gracefully."""
        # Arrange
        job = FetchJob(
            provider="sharadar",
            dataset="price",
            symbol="AAPL",
            spec=RequestSpec(url="https://api.sharadar.com/SEP.json"),
        )

        malformed_payload = b"{invalid json"

        # Act & Assert
        with pytest.raises(json.JSONDecodeError):
            router.parse(job=job, payload=malformed_payload)


class TestRouterEdgeCases:
    """Tests for router edge cases and error conditions."""

    @pytest.fixture
    def router(self) -> SharadarRouter:
        """Create a SharadarRouter instance for testing."""
        return SharadarRouter(
            api_key="test_api_key",
            rate_limit=500,
        )

    @pytest.mark.asyncio
    async def test_router_handles_unknown_dataset_gracefully(
        self, router: SharadarRouter
    ) -> None:
        """Test that router handles unknown dataset names gracefully."""
        # Act & Assert
        with pytest.raises(NotImplementedError, match="Unsupported dataset"):
            async for _ in router.generate_jobs(
                dataset="unknown_dataset", symbols=["AAPL"]
            ):
                pass

    def test_router_maintains_api_rate_limits(self, router: SharadarRouter) -> None:
        """Test that router respects rate limiting configuration.
        
        Args:
            router: SharadarRouter fixture.
        """
        # This would typically be tested with integration tests
        # For unit tests, we verify that the rate limit config is properly set
        assert hasattr(router, "rate_limit")
        assert router.rate_limit == 500
