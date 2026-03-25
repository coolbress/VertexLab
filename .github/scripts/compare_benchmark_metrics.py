from __future__ import annotations

import json
import math
import os
from pathlib import Path

DURATION_PATHS = ("duration_s", "summary.duration_s", "metrics_summary.duration_s")


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


def _resolve_metric(data: dict[str, object], metric_path: str) -> float:
    try:
        return _get_metric(data, metric_path)
    except KeyError:
        if metric_path not in DURATION_PATHS:
            raise
        for fallback_path in DURATION_PATHS:
            if fallback_path == metric_path:
                continue
            try:
                return _get_metric(data, fallback_path)
            except KeyError:
                continue
        started_at = data.get("started_at")
        finished_at = data.get("finished_at")
        if isinstance(started_at, (int, float)) and isinstance(finished_at, (int, float)):
            duration = float(finished_at) - float(started_at)
            if math.isfinite(duration):
                return duration
        raise


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
    try:
        current_raw = _resolve_metric(current_data, metric_path)
    except KeyError as err:
        raise RuntimeError(
            f"Current benchmark metric missing: {metric_path}. "
            f"Tried duration aliases: {', '.join(DURATION_PATHS)}"
        ) from err
    current_value = _validate_metric(
        metric_path=metric_path,
        name="current",
        value=current_raw,
    )
    try:
        baseline_raw = _resolve_metric(baseline_data, metric_path)
    except KeyError:
        print(
            f"Baseline metric missing for {metric_path}; "
            "skip regression check and reseed baseline with current artifact."
        )
        return
    baseline_value = _validate_metric(
        metric_path=metric_path,
        name="baseline",
        value=baseline_raw,
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
