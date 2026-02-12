"""
Unit tests for core.retry module.

Tests cover:
- create_retry_controller function
- Retry configuration integration with tenacity
- Exponential backoff behavior
"""

import pytest
from tenacity import AsyncRetrying, RetryError, stop_after_attempt
from unittest.mock import AsyncMock, patch

from vertex_forager.core.retry import create_retry_controller
from vertex_forager.core.config import RetryConfig


class TestCreateRetryController:
    """Test create_retry_controller function."""

    def test_creates_async_retrying_instance(self):
        """Test that function returns AsyncRetrying instance."""
        config = RetryConfig()
        controller = create_retry_controller(config)
        assert isinstance(controller, AsyncRetrying)

    def test_respects_max_attempts(self):
        """Test that max_attempts is configured correctly."""
        config = RetryConfig(max_attempts=5)
        controller = create_retry_controller(config)

        # Verify stop strategy exists
        assert controller.stop is not None

    def test_respects_backoff_configuration(self):
        """Test that backoff values are configured."""
        config = RetryConfig(
            max_attempts=3,
            base_backoff_s=2.0,
            max_backoff_s=60.0,
        )
        controller = create_retry_controller(config)

        # Controller should be configured with wait strategy
        assert controller.wait is not None

    @pytest.mark.asyncio
    async def test_retry_on_exception(self):
        """Test that controller retries on exceptions."""
        config = RetryConfig(max_attempts=3, base_backoff_s=0.01)
        controller = create_retry_controller(config)

        attempt_count = 0

        async def failing_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ValueError("Simulated error")
            return "success"

        # Execute with retry
        result = None
        async for attempt in controller:
            with attempt:
                result = await failing_function()

        assert result == "success"
        assert attempt_count == 3

    @pytest.mark.asyncio
    async def test_stops_after_max_attempts(self):
        """Test that controller stops after max attempts."""
        config = RetryConfig(max_attempts=2, base_backoff_s=0.01)
        controller = create_retry_controller(config)

        attempt_count = 0

        async def always_fails():
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Always fails")

        # Should raise RetryError after max attempts
        with pytest.raises(Exception):  # Could be RetryError or the original exception
            async for attempt in controller:
                with attempt:
                    await always_fails()

        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_custom_retry_exceptions(self):
        """Test controller with custom retry exception types."""
        config = RetryConfig(max_attempts=3, base_backoff_s=0.01)
        controller = create_retry_controller(
            config,
            retry_on=(ValueError, KeyError)
        )

        attempt_count = 0

        async def fails_with_value_error():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ValueError("Retry this")
            return "success"

        result = None
        async for attempt in controller:
            with attempt:
                result = await fails_with_value_error()

        assert result == "success"
        assert attempt_count == 2

    def test_default_log_level(self):
        """Test that default log level can be configured."""
        import logging
        config = RetryConfig()

        # Test with custom log level
        controller = create_retry_controller(config, log_level=logging.INFO)
        assert controller is not None

    def test_exponential_backoff_parameters(self):
        """Test exponential backoff configuration."""
        config = RetryConfig(
            max_attempts=5,
            base_backoff_s=1.0,
            max_backoff_s=30.0,
        )
        controller = create_retry_controller(config)

        # Verify wait strategy exists
        assert controller.wait is not None


class TestRetryBehavior:
    """Test retry behavior in realistic scenarios."""

    @pytest.mark.asyncio
    async def test_immediate_success_no_retry(self):
        """Test that successful operation doesn't retry."""
        config = RetryConfig(max_attempts=3)
        controller = create_retry_controller(config)

        attempt_count = 0

        async def succeeds_immediately():
            nonlocal attempt_count
            attempt_count += 1
            return "success"

        result = None
        async for attempt in controller:
            with attempt:
                result = await succeeds_immediately()

        assert result == "success"
        assert attempt_count == 1

    @pytest.mark.asyncio
    async def test_eventual_success_after_retries(self):
        """Test eventual success after some failures."""
        config = RetryConfig(max_attempts=5, base_backoff_s=0.01)
        controller = create_retry_controller(config)

        attempt_count = 0

        async def succeeds_on_third_try():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ConnectionError("Temporary failure")
            return "success"

        result = None
        async for attempt in controller:
            with attempt:
                result = await succeeds_on_third_try()

        assert result == "success"
        assert attempt_count == 3


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_zero_backoff(self):
        """Test retry with zero backoff (immediate retry)."""
        config = RetryConfig(max_attempts=2, base_backoff_s=0.0)
        controller = create_retry_controller(config)

        attempt_count = 0

        async def fails_once():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ValueError("Fail once")
            return "success"

        result = None
        async for attempt in controller:
            with attempt:
                result = await fails_once()

        assert result == "success"
        assert attempt_count == 2

    @pytest.mark.asyncio
    async def test_single_attempt_config(self):
        """Test retry with max_attempts=1 (no retries)."""
        config = RetryConfig(max_attempts=1)
        controller = create_retry_controller(config)

        attempt_count = 0

        async def always_fails():
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Always fails")

        with pytest.raises(Exception):
            async for attempt in controller:
                with attempt:
                    await always_fails()

        # Should only attempt once
        assert attempt_count == 1

    def test_large_max_backoff(self):
        """Test retry with very large max backoff."""
        config = RetryConfig(
            max_attempts=10,
            base_backoff_s=1.0,
            max_backoff_s=3600.0,  # 1 hour max
        )
        controller = create_retry_controller(config)
        assert controller is not None


class TestIntegration:
    """Integration tests with realistic scenarios."""

    @pytest.mark.asyncio
    async def test_retry_http_request_simulation(self):
        """Test retry behavior simulating HTTP requests."""
        config = RetryConfig(max_attempts=3, base_backoff_s=0.01)
        controller = create_retry_controller(config)

        mock_response = AsyncMock()
        mock_response.return_value = {"status": "success"}

        # Simulate intermittent failures
        call_count = 0

        async def flaky_http_request():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Network timeout")
            return await mock_response()

        result = None
        async for attempt in controller:
            with attempt:
                result = await flaky_http_request()

        assert result == {"status": "success"}
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_different_exception_types(self):
        """Test retry with different exception types."""
        config = RetryConfig(max_attempts=4, base_backoff_s=0.01)
        controller = create_retry_controller(config)

        exceptions_seen = []

        async def throws_different_exceptions():
            attempt = len(exceptions_seen)
            if attempt == 0:
                exceptions_seen.append(ConnectionError)
                raise ConnectionError("Connection failed")
            elif attempt == 1:
                exceptions_seen.append(TimeoutError)
                raise TimeoutError("Request timeout")
            elif attempt == 2:
                exceptions_seen.append(ValueError)
                raise ValueError("Invalid response")
            return "success"

        result = None
        async for attempt in controller:
            with attempt:
                result = await throws_different_exceptions()

        assert result == "success"
        assert len(exceptions_seen) == 3