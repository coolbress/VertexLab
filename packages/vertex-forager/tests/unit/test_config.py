"""
Unit tests for core.config module.

Tests cover:
- Configuration classes (EngineConfig, RetryConfig, RequestSpec, etc.)
- Data models (FetchJob, FramePacket, RunResult, ParseResult)
- Validation logic
- Property calculations
"""

from datetime import datetime, timezone, date
from unittest.mock import patch

import polars as pl
import pytest
from pydantic import ValidationError

from vertex_forager.core.config import (
    EngineConfig,
    RetryConfig,
    HttpMethod,
    RequestAuth,
    RequestSpec,
    FetchJob,
    FramePacket,
    RunResult,
    ParseResult,
)


class TestRetryConfig:
    """Test RetryConfig model."""

    def test_default_values(self):
        """Verify default retry configuration values."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_backoff_s == 1.0
        assert config.max_backoff_s == 30.0

    def test_custom_values(self):
        """Test custom retry configuration."""
        config = RetryConfig(max_attempts=5, base_backoff_s=2.0, max_backoff_s=60.0)
        assert config.max_attempts == 5
        assert config.base_backoff_s == 2.0
        assert config.max_backoff_s == 60.0


class TestHttpMethod:
    """Test HttpMethod enum."""

    def test_get_method(self):
        """Test GET method enum value."""
        assert HttpMethod.GET == "GET"
        assert HttpMethod.GET.value == "GET"

    def test_post_method(self):
        """Test POST method enum value."""
        assert HttpMethod.POST == "POST"
        assert HttpMethod.POST.value == "POST"


class TestRequestAuth:
    """Test RequestAuth model."""

    def test_default_auth_none(self):
        """Test default authentication is 'none'."""
        auth = RequestAuth()
        assert auth.kind == "none"
        assert auth.token is None
        assert auth.header_name is None
        assert auth.query_param is None

    def test_bearer_auth(self):
        """Test bearer token authentication."""
        auth = RequestAuth(kind="bearer", token="test_token_123")
        assert auth.kind == "bearer"
        assert auth.token == "test_token_123"

    def test_header_auth(self):
        """Test header-based authentication."""
        auth = RequestAuth(kind="header", token="api_key_xyz", header_name="X-API-Key")
        assert auth.kind == "header"
        assert auth.token == "api_key_xyz"
        assert auth.header_name == "X-API-Key"

    def test_query_param_auth(self):
        """Test query parameter authentication."""
        auth = RequestAuth(kind="query", token="secret", query_param="api_key")
        assert auth.kind == "query"
        assert auth.token == "secret"
        assert auth.query_param == "api_key"


class TestRequestSpec:
    """Test RequestSpec model."""

    def test_minimal_request_spec(self):
        """Test request spec with minimal required fields."""
        spec = RequestSpec(url="https://api.example.com/data")
        assert spec.method == HttpMethod.GET
        assert spec.url == "https://api.example.com/data"
        assert spec.params == {}
        assert spec.headers == {}
        assert spec.json_body is None
        assert spec.data is None
        assert spec.timeout_s == 30.0
        assert spec.auth.kind == "none"

    def test_full_request_spec(self):
        """Test request spec with all fields."""
        auth = RequestAuth(kind="bearer", token="token123")
        spec = RequestSpec(
            method=HttpMethod.POST,
            url="https://api.example.com/data",
            params={"page": "1"},
            headers={"User-Agent": "test"},
            json_body={"key": "value"},
            timeout_s=60.0,
            auth=auth,
        )
        assert spec.method == HttpMethod.POST
        assert spec.url == "https://api.example.com/data"
        assert spec.params == {"page": "1"}
        assert spec.headers == {"User-Agent": "test"}
        assert spec.json_body == {"key": "value"}
        assert spec.timeout_s == 60.0
        assert spec.auth.kind == "bearer"

    def test_request_spec_with_data_bytes(self):
        """Test request spec with binary data."""
        spec = RequestSpec(url="https://api.example.com/upload", data=b"binary_data")
        assert spec.data == b"binary_data"


class TestFetchJob:
    """Test FetchJob model."""

    def test_minimal_fetch_job(self):
        """Test fetch job with minimal fields."""
        spec = RequestSpec(url="https://api.example.com/data")
        job = FetchJob(provider="test", dataset="prices", spec=spec)
        assert job.provider == "test"
        assert job.dataset == "prices"
        assert job.symbol is None
        assert job.spec.url == "https://api.example.com/data"
        assert job.context == {}

    def test_fetch_job_with_symbol(self):
        """Test fetch job with symbol."""
        spec = RequestSpec(url="https://api.example.com/data")
        job = FetchJob(provider="sharadar", dataset="price", symbol="AAPL", spec=spec)
        assert job.provider == "sharadar"
        assert job.dataset == "price"
        assert job.symbol == "AAPL"

    def test_fetch_job_with_context(self):
        """Test fetch job with context metadata."""
        spec = RequestSpec(url="https://api.example.com/data")
        context = {"pagination": {"cursor": "abc123"}}
        job = FetchJob(
            provider="test",
            dataset="data",
            spec=spec,
            context=context,
        )
        assert job.context == {"pagination": {"cursor": "abc123"}}


class TestFramePacket:
    """Test FramePacket model."""

    def test_frame_packet_creation(self):
        """Test creating a frame packet."""
        frame = pl.DataFrame({"ticker": ["AAPL"], "price": [150.0]})
        observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="test_table",
            frame=frame,
            observed_at=observed_at,
        )
        assert packet.provider == "test"
        assert packet.table == "test_table"
        assert packet.frame.shape == (1, 2)
        assert packet.observed_at == observed_at
        assert packet.partition_date is None
        assert packet.context == {}

    def test_frame_packet_with_partition_date(self):
        """Test frame packet with partition date."""
        frame = pl.DataFrame({"ticker": ["AAPL"]})
        observed_at = datetime.now(tz=timezone.utc)
        partition = date(2024, 1, 1)
        packet = FramePacket(
            provider="test",
            table="test_table",
            frame=frame,
            observed_at=observed_at,
            partition_date=partition,
        )
        assert packet.partition_date == partition

    def test_frame_packet_with_context(self):
        """Test frame packet with context metadata."""
        frame = pl.DataFrame({"ticker": ["AAPL"]})
        observed_at = datetime.now(tz=timezone.utc)
        context = {"dataset": "price", "source": "api"}
        packet = FramePacket(
            provider="test",
            table="test_table",
            frame=frame,
            observed_at=observed_at,
            context=context,
        )
        assert packet.context == context


class TestEngineConfig:
    """Test EngineConfig model."""

    def test_minimal_config(self):
        """Test engine config with minimal required fields."""
        config = EngineConfig(requests_per_minute=100)
        assert config.requests_per_minute == 100
        assert config.concurrency is None
        assert config.retry.max_attempts == 3
        assert config.flush_threshold_rows == 500_000

    def test_config_with_concurrency(self):
        """Test engine config with explicit concurrency."""
        config = EngineConfig(requests_per_minute=500, concurrency=20)
        assert config.requests_per_minute == 500
        assert config.concurrency == 20
        assert config.fetch_concurrency == 20

    def test_config_with_custom_retry(self):
        """Test engine config with custom retry configuration."""
        retry = RetryConfig(max_attempts=5, base_backoff_s=2.0)
        config = EngineConfig(requests_per_minute=100, retry=retry)
        assert config.retry.max_attempts == 5
        assert config.retry.base_backoff_s == 2.0

    def test_fetch_concurrency_property(self):
        """Test fetch_concurrency property alias."""
        config = EngineConfig(requests_per_minute=100, concurrency=15)
        assert config.fetch_concurrency == 15

    @patch("os.sysconf")
    def test_queue_max_calculation(self, mock_sysconf):
        """Test queue_max calculation based on system memory."""
        # Mock system with 8GB RAM (8 * 1024^3 bytes)
        # SC_PHYS_PAGES * SC_PAGE_SIZE = Total RAM
        total_ram = 8 * 1024 * 1024 * 1024
        page_size = 4096
        pages = total_ram // page_size

        mock_sysconf.side_effect = lambda key: {
            'SC_PHYS_PAGES': pages,
            'SC_PAGE_SIZE': page_size,
        }[key]

        config = EngineConfig(requests_per_minute=100)
        queue_max = config.queue_max

        # 5% of 8GB = 400MB
        # 400MB / 5MB per packet = 80 packets
        # Should be within min/max bounds (100, 2000)
        assert 100 <= queue_max <= 2000

    def test_queue_max_fallback(self):
        """Test queue_max fallback when system info unavailable."""
        with patch("os.sysconf", side_effect=ValueError):
            config = EngineConfig(requests_per_minute=100)
            assert config.queue_max == 500

    def test_validate_positive_rpm(self):
        """Test validation requires positive requests_per_minute."""
        config = EngineConfig(requests_per_minute=100)
        config.validate()  # Should not raise

    def test_validate_negative_rpm_raises(self):
        """Test validation fails with negative RPM."""
        config = EngineConfig(requests_per_minute=-10)
        with pytest.raises(ValueError, match="requests_per_minute must be positive"):
            config.validate()

    def test_validate_zero_rpm_raises(self):
        """Test validation fails with zero RPM."""
        config = EngineConfig(requests_per_minute=0)
        with pytest.raises(ValueError, match="requests_per_minute must be positive"):
            config.validate()


class TestRunResult:
    """Test RunResult model."""

    def test_empty_run_result(self):
        """Test creating an empty run result."""
        result = RunResult(provider="test")
        assert result.provider == "test"
        assert result.tables == {}
        assert result.errors == []

    def test_add_rows(self):
        """Test adding rows to run result."""
        result = RunResult(provider="test")
        result.add_rows(table="test_table", rows=100)
        assert result.tables["test_table"] == 100

    def test_add_rows_accumulates(self):
        """Test adding rows accumulates across multiple calls."""
        result = RunResult(provider="test")
        result.add_rows(table="test_table", rows=100)
        result.add_rows(table="test_table", rows=50)
        result.add_rows(table="test_table", rows=25)
        assert result.tables["test_table"] == 175

    def test_add_rows_multiple_tables(self):
        """Test tracking multiple tables."""
        result = RunResult(provider="test")
        result.add_rows(table="table1", rows=100)
        result.add_rows(table="table2", rows=200)
        assert result.tables["table1"] == 100
        assert result.tables["table2"] == 200

    def test_errors_list(self):
        """Test tracking errors."""
        result = RunResult(provider="test", errors=["error1", "error2"])
        assert len(result.errors) == 2
        assert "error1" in result.errors
        assert "error2" in result.errors


class TestParseResult:
    """Test ParseResult dataclass."""

    def test_empty_parse_result(self):
        """Test creating empty parse result."""
        result = ParseResult(packets=[], next_jobs=[])
        assert result.packets == []
        assert result.next_jobs == []

    def test_parse_result_with_packets(self):
        """Test parse result with data packets."""
        frame = pl.DataFrame({"ticker": ["AAPL"]})
        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="test_table",
            frame=frame,
            observed_at=observed_at,
        )
        result = ParseResult(packets=[packet], next_jobs=[])
        assert len(result.packets) == 1
        assert result.packets[0] == packet

    def test_parse_result_with_next_jobs(self):
        """Test parse result with pagination jobs."""
        spec = RequestSpec(url="https://api.example.com/data")
        job = FetchJob(provider="test", dataset="data", spec=spec)
        result = ParseResult(packets=[], next_jobs=[job])
        assert len(result.next_jobs) == 1
        assert result.next_jobs[0] == job

    def test_parse_result_immutable(self):
        """Test ParseResult is frozen (immutable)."""
        result = ParseResult(packets=[], next_jobs=[])
        with pytest.raises(AttributeError):
            result.packets = []  # type: ignore


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_frame_packet_with_empty_dataframe(self):
        """Test frame packet with empty DataFrame."""
        frame = pl.DataFrame()
        observed_at = datetime.now(tz=timezone.utc)
        packet = FramePacket(
            provider="test",
            table="test_table",
            frame=frame,
            observed_at=observed_at,
        )
        assert packet.frame.is_empty()

    def test_request_spec_empty_params_and_headers(self):
        """Test request spec handles empty dicts correctly."""
        spec = RequestSpec(url="https://api.example.com", params={}, headers={})
        assert spec.params == {}
        assert spec.headers == {}

    def test_engine_config_very_high_rpm(self):
        """Test engine config with very high RPM."""
        config = EngineConfig(requests_per_minute=10000)
        assert config.requests_per_minute == 10000
        config.validate()  # Should not raise

    def test_fetch_job_model_copy(self):
        """Test FetchJob can be copied (for pagination)."""
        spec = RequestSpec(url="https://api.example.com")
        job = FetchJob(provider="test", dataset="data", spec=spec)
        job_copy = job.model_copy(deep=True)
        assert job_copy.provider == job.provider
        assert job_copy.dataset == job.dataset
        assert job_copy.spec.url == job.spec.url


class TestNegativeCases:
    """Test negative cases and error handling."""

    def test_request_spec_missing_url_raises(self):
        """Test RequestSpec requires URL."""
        with pytest.raises(ValidationError):
            RequestSpec()  # type: ignore

    def test_fetch_job_missing_required_fields_raises(self):
        """Test FetchJob requires provider, dataset, and spec."""
        with pytest.raises(ValidationError):
            FetchJob(provider="test")  # type: ignore

    def test_frame_packet_missing_required_fields_raises(self):
        """Test FramePacket requires all mandatory fields."""
        with pytest.raises(ValidationError):
            FramePacket(provider="test", table="table")  # type: ignore

    def test_engine_config_invalid_concurrency_type(self):
        """Test engine config validates concurrency type."""
        with pytest.raises(ValidationError):
            EngineConfig(requests_per_minute=100, concurrency="invalid")  # type: ignore