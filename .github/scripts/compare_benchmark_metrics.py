from __future__ import annotations

import json
import math
import os
from pathlib import Path


def _get_metric(data: dict[str, object], metric_path: str) -> float:
    current: object = data
    for part in metric_path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(metric_path)
        current = current[part]
    return float(current)


def _validate_metric(*, metric_path: str, name: str, value: float) -> float:
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(
            f"Invalid {name} metric value for {metric_path}: {value}. "
            "Expected a finite positive number."
        )
    return value


def _validate_threshold(raw: str) -> float:
    try:
        threshold = float(raw)
    except ValueError as err:
        raise RuntimeError(
            f"Invalid BENCHMARK_MAX_REGRESSION value: {raw}. Expected a finite number >= 0."
        ) from err
    if not math.isfinite(threshold) or threshold < 0:
        raise RuntimeError(
            f"Invalid BENCHMARK_MAX_REGRESSION value: {raw}. Expected a finite number >= 0."
        )
    return threshold


def main() -> None:
    out_dir = Path(os.getenv("VF_PROFILE_OUTPUT_DIR", str(Path.cwd() / "output" / "forager-profiles")))
    current_path = out_dir / "profile_metrics.json"
    baseline_path = out_dir / "baseline" / "profile_metrics.json"
    metric_path = os.getenv("BENCHMARK_METRIC_PATH", "duration_s")
    threshold = _validate_threshold(os.getenv("BENCHMARK_MAX_REGRESSION", "0.20"))

    if not current_path.exists():
        raise FileNotFoundError(f"Current benchmark result missing: {current_path}")
    if not baseline_path.exists():
        print("No baseline metrics found; seed baseline with current run.")
        return

    current_data = json.loads(current_path.read_text())
    baseline_data = json.loads(baseline_path.read_text())
    current_value = _validate_metric(
        metric_path=metric_path,
        name="current",
        value=_get_metric(current_data, metric_path),
    )
    baseline_value = _validate_metric(
        metric_path=metric_path,
        name="baseline",
        value=_get_metric(baseline_data, metric_path),
    )

    regression = (current_value - baseline_value) / baseline_value
    regression_pct = regression * 100.0
    threshold_pct = threshold * 100.0
    print(
        f"Benchmark regression check: metric={metric_path} baseline={baseline_value:.6f} "
        f"current={current_value:.6f} regression={regression_pct:.2f}% threshold={threshold_pct:.2f}%"
    )

    if regression > threshold:
        raise RuntimeError(
            f"Benchmark regression exceeded threshold: {regression_pct:.2f}% > {threshold_pct:.2f}%"
        )


if __name__ == "__main__":
    main()
