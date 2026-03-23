from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st
import pytest

from vertex_forager.core.config import EngineConfig
from vertex_forager.core.controller import FlowController


@given(
    rpm=st.integers(min_value=1, max_value=6000),
    floor=st.integers(min_value=1, max_value=10000),
)
def test_engineconfig_rpm_floor_constraint(rpm: int, floor: int) -> None:
    cfg = EngineConfig(requests_per_minute=rpm, concurrency=1, rpm_floor=max(1, floor))
    if cfg.rpm_floor <= cfg.requests_per_minute:
        cfg.assert_valid()
    else:
        with pytest.raises(ValueError):
            cfg.assert_valid()


@given(
    err=st.floats(allow_infinity=True, allow_nan=True, width=32),
    floor=st.integers(min_value=1, max_value=10000),
    rpm=st.integers(min_value=1, max_value=6000),
)
def test_flowcontroller_clamps_threshold_and_floor(err: float, floor: int, rpm: int) -> None:
    fc = FlowController(
        requests_per_minute=rpm,
        concurrency_limit=1,
        downshift_enabled=True,
        error_rate_threshold=err,
        rpm_floor=floor,
    )
    # Internal clamps: threshold in [0,1], floor in [1, rpm]
    # Not part of public API, but we can infer via behavior: ensure no exceptions and values bounded
    assert 0.0 <= fc._error_threshold <= 1.0  # type: ignore[attr-defined]
    assert 1 <= fc._rpm_floor <= rpm  # type: ignore[attr-defined]
