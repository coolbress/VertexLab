from __future__ import annotations

import asyncio
import hashlib
import itertools
import os
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


def _parse_positive_int_list(value: str | None, default: list[int], field_name: str) -> list[int]:
    if not value:
        return default
    parsed: list[int] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            raise ValueError(f"Invalid empty value in {field_name}: '{value}'")
        parsed_value = int(token)
        if parsed_value <= 0:
            raise ValueError(f"Invalid value '{token}' in {field_name}: must be positive")
        parsed.append(parsed_value)
    return parsed


def build_sweep_combinations(
    *,
    concurrency_list: str | None,
    flush_rows_list: str | None,
    keepalive_list: str | None,
    connections_list: str | None,
    timeout_list: str | None,
    sample_count: int | None,
    sample_seed: int,
) -> tuple[list[dict[str, Any]], str]:
    concs = _parse_positive_int_list(concurrency_list, [8, 12, 16, 20, 24], "concurrency_list")
    flushes = _parse_positive_int_list(flush_rows_list, [100_000, 150_000, 200_000, 250_000, 300_000], "flush_rows")
    keepalives = _parse_positive_int_list(keepalive_list, [100, 150, 200, 250], "keepalive_list")
    connections = _parse_positive_int_list(connections_list, [200, 300, 400, 500], "connections_list")
    timeouts = _parse_positive_int_list(timeout_list, [30, 45], "timeout_list")
    combos = [
        {
            "VF_CONCURRENCY": c,
            "VF_FLUSH_THRESHOLD_ROWS": f,
            "VF_HTTP_MAX_KEEPALIVE": k,
            "VF_HTTP_MAX_CONNECTIONS": m,
            "VF_HTTP_TIMEOUT_S": t,
        }
        for c, f, k, m, t in itertools.product(concs, flushes, keepalives, connections, timeouts)
    ]
    count = sample_count if sample_count is not None else 50
    if 0 < count < len(combos):
        ranked = sorted(
            range(len(combos)),
            key=lambda i: int.from_bytes(hashlib.sha256(f"{sample_seed}:{i}".encode()).digest()[:8], "big"),
        )
        selected = [combos[i] for i in ranked[:count]]
        return selected, f"Sampling {count} combinations from {len(combos)} total (seed={sample_seed})."
    return combos, f"Running all {len(combos)} combinations."


def _collect_metrics(run_result: Any) -> dict[str, Any]:
    return {
        "summary": getattr(run_result, "metrics_summary", {}),
        "counters": getattr(run_result, "metrics_counters", {}),
        "errors": getattr(run_result, "errors", []),
    }


def _measure_yfinance_price(
    *,
    combo_db_path: Path,
    tickers: list[str],
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    from vertex_forager.providers.yfinance.client import YFinanceClient

    client = YFinanceClient(rate_limit=60, structured_logs=False)
    t0 = time.monotonic()
    run_result = asyncio.run(
        client.get_price_data(
            tickers=tickers,
            connect_db=combo_db_path,
            show_progress=False,
            start_date=start_date,
            end_date=end_date,
        )
    )
    t1 = time.monotonic()
    return {"duration_s": round(t1 - t0, 3), "metrics": _collect_metrics(run_result)}


def _measure_yfinance_financials(*, combo_db_path: Path, tickers: list[str]) -> dict[str, Any]:
    from vertex_forager.providers.yfinance.client import YFinanceClient

    client = YFinanceClient(rate_limit=60, structured_logs=False)
    t0 = time.monotonic()
    run_result = asyncio.run(
        client.get_financials(
            kind="income_stmt",
            period="annual",
            tickers=tickers,
            connect_db=combo_db_path,
            show_progress=False,
        )
    )
    t1 = time.monotonic()
    return {"duration_s": round(t1 - t0, 3), "metrics": _collect_metrics(run_result)}


def _measure_sharadar(
    *,
    combo_db_path: Path,
    api_key: str,
    tickers: list[str],
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    from vertex_forager.providers.sharadar.client import SharadarClient

    client = SharadarClient(api_key=api_key, rate_limit=60, structured_logs=False)
    t0 = time.monotonic()
    run_result = asyncio.run(
        client.get_fundamental_data(
            tickers=tickers,
            connect_db=combo_db_path,
            dimension="MRT",
            start_date=start_date,
            end_date=end_date,
        )
    )
    t1 = time.monotonic()
    return {"duration_s": round(t1 - t0, 3), "metrics": _collect_metrics(run_result)}


def _restore_environment(original_env: dict[str, str]) -> None:
    current_env = dict(os.environ)
    for key in current_env.keys() - original_env.keys():
        os.environ.pop(key, None)
    for key, value in original_env.items():
        os.environ[key] = value
    os.environ.setdefault("VF_METRICS_ENABLED", "1")


def _run_single_combo(
    *,
    idx: int,
    cfg: dict[str, Any],
    output_dir: Path,
    include_sharadar: bool,
    logger: Any,
    yf_tickers_price: list[str],
    yf_tickers_fin: list[str],
    yf_start: str | None,
    yf_end: str | None,
    sh_key: str | None,
    sh_tickers: list[str],
    sh_start: str | None,
    sh_end: str | None,
    set_env: Any,
) -> dict[str, Any]:
    combo_db_path = output_dir / f"profile_sweep_{idx}.duckdb"
    if combo_db_path.exists():
        combo_db_path.unlink()
    set_env(cfg)
    entry: dict[str, Any] = {"env": dict(cfg), "measurements": {}}
    try:
        entry["measurements"]["yfinance_price"] = _measure_yfinance_price(
            combo_db_path=combo_db_path,
            tickers=yf_tickers_price,
            start_date=yf_start,
            end_date=yf_end,
        )
    except Exception as exc:
        logger.warning("YFinance Price sweep error: %s", exc)
        entry["measurements"]["yfinance_price"] = {"error": str(exc)}
    try:
        entry["measurements"]["yfinance_financials"] = _measure_yfinance_financials(
            combo_db_path=combo_db_path,
            tickers=yf_tickers_fin,
        )
    except Exception as exc:
        logger.warning("YFinance Financials sweep error: %s", exc)
        entry["measurements"]["yfinance_financials"] = {"error": str(exc)}
    if include_sharadar and sh_key:
        try:
            entry["measurements"]["sharadar_sf1_mrt"] = _measure_sharadar(
                combo_db_path=combo_db_path,
                api_key=sh_key,
                tickers=sh_tickers,
                start_date=sh_start,
                end_date=sh_end,
            )
        except Exception as exc:
            logger.warning("Sharadar sweep error: %s", exc)
            entry["measurements"]["sharadar_sf1_mrt"] = {"error": str(exc)}
    if combo_db_path.exists():
        try:
            combo_db_path.unlink()
        except OSError as exc:
            logger.warning("Failed to cleanup temp DB %s: %s", combo_db_path, exc)
    return entry


def run_sweep_measurements(
    *,
    combos: list[dict[str, Any]],
    output_dir: Path,
    include_sharadar: bool,
    logger: Any,
) -> dict[str, Any]:
    from vertex_forager.utils import load_tickers_env, set_env

    yf_tickers_price = load_tickers_env("YF_TICKERS_PRICE", ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"])
    yf_tickers_fin = load_tickers_env("YF_TICKERS_FIN", ["AAPL", "MSFT", "NVDA"])
    yf_start = os.getenv("YF_PRICE_START_DATE")
    yf_end = os.getenv("YF_PRICE_END_DATE")
    sh_key = os.getenv("SHARADAR_API_KEY") if include_sharadar else None
    sh_tickers = load_tickers_env("SH_TICKERS", ["AAPL", "MSFT", "NVDA"])
    sh_start = os.getenv("SH_START_DATE")
    sh_end = os.getenv("SH_END_DATE")
    results: dict[str, Any] = {"runs": []}
    original_env = os.environ.copy()
    try:
        for idx, cfg in enumerate(combos):
            results["runs"].append(
                _run_single_combo(
                    idx=idx,
                    cfg=cfg,
                    output_dir=output_dir,
                    include_sharadar=include_sharadar,
                    logger=logger,
                    yf_tickers_price=yf_tickers_price,
                    yf_tickers_fin=yf_tickers_fin,
                    yf_start=yf_start,
                    yf_end=yf_end,
                    sh_key=sh_key,
                    sh_tickers=sh_tickers,
                    sh_start=sh_start,
                    sh_end=sh_end,
                    set_env=set_env,
                )
            )
            _restore_environment(original_env)
    finally:
        _restore_environment(original_env)
    return results


def _score_run(
    *,
    run: dict[str, Any],
    run_key: str,
    rank_by: str,
    rank_alpha: float,
    rank_error_penalty: float,
    logger: Any,
) -> float:
    measurement = run["measurements"].get(run_key, {})
    duration = measurement.get("duration_s", float("inf"))
    metrics = measurement.get("metrics", {})
    errors = metrics.get("errors", [])
    err_cnt = len(errors) if isinstance(errors, list) else 0
    try:
        score = float(duration)
    except (ValueError, TypeError):
        score = float("inf")
    if rank_by == "duration_p95":
        summary = metrics.get("summary", {})
        p95 = summary.get("http_duration_s_p95", duration)
        try:
            score += float(rank_alpha) * float(p95)
        except (ValueError, TypeError) as exc:
            logger.warning("Error computing p95 score: %s, inputs: alpha=%s, p95=%s", exc, rank_alpha, p95)
    if err_cnt > 0:
        try:
            score += float(err_cnt) * float(rank_error_penalty)
        except (ValueError, TypeError) as exc:
            logger.warning(
                "Error computing penalty score: %s, inputs: err_cnt=%s, penalty=%s",
                exc,
                err_cnt,
                rank_error_penalty,
            )
            score += float(err_cnt) * 5.0
    return score


def _best_for_key(
    *,
    results: dict[str, Any],
    run_key: str,
    rank_by: str,
    rank_alpha: float,
    rank_error_penalty: float,
    logger: Any,
) -> dict[str, Any]:
    raw_runs = results.get("runs", [])
    runs: list[dict[str, Any]] = []
    if isinstance(raw_runs, list):
        for candidate in raw_runs:
            if not isinstance(candidate, dict):
                continue
            measurements = candidate.get("measurements", {})
            if not isinstance(measurements, dict):
                continue
            measurement = measurements.get(run_key, {})
            if not isinstance(measurement, dict):
                continue
            if measurement.get("error"):
                continue
            runs.append(candidate)
    if not runs:
        return {}
    ranked = sorted(
        runs,
        key=lambda run: _score_run(
            run=run,
            run_key=run_key,
            rank_by=rank_by,
            rank_alpha=rank_alpha,
            rank_error_penalty=rank_error_penalty,
            logger=logger,
        ),
    )
    return ranked[0]


def score_and_rank_results(
    *,
    results: dict[str, Any],
    rank_by: str,
    rank_alpha: float,
    rank_error_penalty: float,
    logger: Any,
) -> dict[str, Any]:
    results["best"] = {
        "yfinance_price": _best_for_key(
            results=results,
            run_key="yfinance_price",
            rank_by=rank_by,
            rank_alpha=rank_alpha,
            rank_error_penalty=rank_error_penalty,
            logger=logger,
        ),
        "yfinance_financials": _best_for_key(
            results=results,
            run_key="yfinance_financials",
            rank_by=rank_by,
            rank_alpha=rank_alpha,
            rank_error_penalty=rank_error_penalty,
            logger=logger,
        ),
    }
    return results
