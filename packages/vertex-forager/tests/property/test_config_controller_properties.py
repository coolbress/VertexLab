from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from vertex_forager.core.config import AdaptiveThrottleConfig, ResolvedClientConfig
from vertex_forager.core.controller import FlowController


@given(
    rpm=st.integers(min_value=1, max_value=6000),
    floor_ratio=st.floats(min_value=0.0, max_value=1.0),
)
def test_rpm_floor_ratio_resolves_to_absolute_floor(rpm: int, floor_ratio: float) -> None:
    cfg = ResolvedClientConfig(
        requests_per_minute=rpm,
        concurrency=1,
        adaptive_throttle=AdaptiveThrottleConfig(rpm_floor_ratio=floor_ratio),
    )
    assert cfg.rpm_floor_ratio == floor_ratio


@given(
    err=st.floats(allow_infinity=True, allow_nan=True, width=32),
    floor_ratio=st.floats(min_value=0.0, max_value=1.0),
    rpm=st.integers(min_value=1, max_value=6000),
)
def test_flowcontroller_clamps_threshold_and_floor(err: float, floor_ratio: float, rpm: int) -> None:
    fc = FlowController(
        requests_per_minute=rpm,
        concurrency_limit=1,
        adaptive_throttle_enabled=True,
        error_rate_threshold=err,
        rpm_floor_ratio=floor_ratio,
    )
    assert 0.0 <= fc._error_threshold <= 1.0  # type: ignore[attr-defined]
    assert 1 <= fc._rpm_floor <= rpm  # type: ignore[attr-defined]


@given(
    rpm=st.integers(min_value=1, max_value=6000),
    recovery_factor=st.floats(min_value=0.0, max_value=1.0),
)
def test_recovery_factor_resolves_to_absolute_step(rpm: int, recovery_factor: float) -> None:
    fc = FlowController(
        requests_per_minute=rpm,
        concurrency_limit=1,
        adaptive_throttle_enabled=True,
        recovery_factor=recovery_factor,
    )
    expected_step = max(1, int(rpm * recovery_factor))
    assert fc._recovery_step == expected_step
