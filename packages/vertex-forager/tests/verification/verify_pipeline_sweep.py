import json
import os
from pathlib import Path
import time
from typing import Any

from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.utils import load_tickers_env

_YF_OPTIONAL_DEPS_MSG = "install optional deps with `pip install vertex-forager[yfinance]`"


def _get_yfinance_client(client_config: dict[str, Any]) -> Any:
    try:
        from vertex_forager.providers.yfinance.client import YFinanceClient
    except ImportError as err:
        if err.name in {"pandas", "yfinance"}:
            raise RuntimeError(f"Skipping verification: {_YF_OPTIONAL_DEPS_MSG}") from err
        raise
    return YFinanceClient(rate_limit=60, structured_logs=False, **client_config)


def _resolve_paths() -> tuple[Path, Path, Path]:
    out_dir_env = os.getenv("VF_PROFILE_OUTPUT_DIR")
    out_dir = Path(out_dir_env) if out_dir_env else (Path.cwd() / "output" / "forager-profiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir, out_dir / "profile_sweep.duckdb", out_dir / "profile_sweep_results.json"


def _build_combos() -> list[dict[str, Any]]:
    return [
        {
            "metrics_enabled": True,
            "concurrency": 8,
            "flush_threshold_rows": 100_000,
            "http_timeout_s": 30,
            "limits": {"max_keepalive_connections": 100, "max_connections": 200},
        },
        {
            "metrics_enabled": True,
            "concurrency": 12,
            "flush_threshold_rows": 150_000,
            "http_timeout_s": 30,
            "limits": {"max_keepalive_connections": 150, "max_connections": 300},
        },
        {
            "metrics_enabled": True,
            "concurrency": 16,
            "flush_threshold_rows": 200_000,
            "http_timeout_s": 45,
            "limits": {"max_keepalive_connections": 200, "max_connections": 400},
        },
        {
            "metrics_enabled": True,
            "concurrency": 20,
            "flush_threshold_rows": 250_000,
            "http_timeout_s": 45,
            "limits": {"max_keepalive_connections": 200, "max_connections": 400},
        },
        {
            "metrics_enabled": True,
            "concurrency": 24,
            "flush_threshold_rows": 300_000,
            "http_timeout_s": 45,
            "limits": {"max_keepalive_connections": 250, "max_connections": 500},
        },
    ]


def _collect_env_inputs() -> dict[str, Any]:
    return {
        "yf_tickers_price": load_tickers_env("YF_TICKERS_PRICE", ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]),
        "yf_tickers_fin": load_tickers_env("YF_TICKERS_FIN", ["AAPL", "MSFT", "NVDA"]),
        "yf_start": os.getenv("YF_PRICE_START_DATE"),
        "yf_end": os.getenv("YF_PRICE_END_DATE"),
        "sh_key": os.getenv("SHARADAR_API_KEY"),
        "sh_tickers": load_tickers_env("SH_TICKERS", ["AAPL", "MSFT", "NVDA"]),
        "sh_start": os.getenv("SH_START_DATE"),
        "sh_end": os.getenv("SH_END_DATE"),
    }


def _measure_yfinance_price(*, db_path: Path, inputs: dict[str, Any], client_config: dict[str, Any]) -> dict[str, Any]:
    yfc = _get_yfinance_client(client_config)
    t0 = time.monotonic()
    try:
        yf_price = yfc.get_price_data(
            tickers=inputs["yf_tickers_price"],
            connect_db=db_path,
            show_progress=False,
            start_date=inputs["yf_start"],
            end_date=inputs["yf_end"],
        )
    except ImportError as err:
        if err.name in {"pandas", "yfinance"}:
            raise RuntimeError(f"Skipping verification: {_YF_OPTIONAL_DEPS_MSG}") from err
        raise
    t1 = time.monotonic()
    return {
        "duration_s": round(t1 - t0, 3),
        "metrics": {
            "summary": getattr(yf_price, "metrics_summary", {}),
            "counters": getattr(yf_price, "metrics_counters", {}),
        },
    }


def _measure_yfinance_financials(
    *,
    db_path: Path,
    inputs: dict[str, Any],
    client_config: dict[str, Any],
) -> dict[str, Any]:
    yfc = _get_yfinance_client(client_config)
    t0 = time.monotonic()
    try:
        yf_fin = yfc.get_financials(
            kind="income_stmt",
            period="annual",
            tickers=inputs["yf_tickers_fin"],
            connect_db=db_path,
            show_progress=False,
        )
    except ImportError as err:
        if err.name in {"pandas", "yfinance"}:
            raise RuntimeError(f"Skipping verification: {_YF_OPTIONAL_DEPS_MSG}") from err
        raise
    t1 = time.monotonic()
    return {
        "duration_s": round(t1 - t0, 3),
        "metrics": {
            "summary": getattr(yf_fin, "metrics_summary", {}),
            "counters": getattr(yf_fin, "metrics_counters", {}),
        },
    }


def _measure_sharadar(*, db_path: Path, inputs: dict[str, Any], client_config: dict[str, Any]) -> dict[str, Any]:
    shc = SharadarClient(api_key=inputs["sh_key"], rate_limit=60, structured_logs=False, **client_config)
    t0 = time.monotonic()
    sh_fin = shc.get_fundamental_data(
        tickers=inputs["sh_tickers"],
        connect_db=db_path,
        dimension="MRT",
        start_date=inputs["sh_start"],
        end_date=inputs["sh_end"],
    )
    t1 = time.monotonic()
    return {
        "duration_s": round(t1 - t0, 3),
        "metrics": {
            "summary": getattr(sh_fin, "metrics_summary", {}),
            "counters": getattr(sh_fin, "metrics_counters", {}),
        },
    }


def _run_single_combo(*, cfg: dict[str, Any], db_path: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    run_entry: dict[str, Any] = {"client_config": dict(cfg), "measurements": {}}
    try:
        run_entry["measurements"]["yfinance_price"] = _measure_yfinance_price(
            db_path=db_path,
            inputs=inputs,
            client_config=cfg,
        )
    except Exception as e:
        print(f"yfinance_price verification failed: {e}")
        run_entry["measurements"]["yfinance_price"] = {"error": str(e)}
    try:
        run_entry["measurements"]["yfinance_financials"] = _measure_yfinance_financials(
            db_path=db_path,
            inputs=inputs,
            client_config=cfg,
        )
    except Exception as e:
        print(f"yfinance_financials verification failed: {e}")
        run_entry["measurements"]["yfinance_financials"] = {"error": str(e)}
    if inputs["sh_key"]:
        try:
            run_entry["measurements"]["sharadar_sf1_mrt"] = _measure_sharadar(
                db_path=db_path,
                inputs=inputs,
                client_config=cfg,
            )
        except Exception as e:
            print(f"Sharadar verification skipped due to error: {e}")
            run_entry["measurements"]["sharadar_sf1_mrt"] = {"error": str(e)}
    return run_entry


def _best_run(results: dict[str, Any], run_key: str) -> dict[str, Any]:
    ranked = sorted(
        results["runs"],
        key=lambda r: r["measurements"].get(run_key, {}).get("duration_s", float("inf")),
    )
    return ranked[0] if ranked else {}


def _finalize_reports(*, results: dict[str, Any], out_dir: Path, report_path: Path) -> None:
    results["best"] = {
        "yfinance_price": _best_run(results, "yfinance_price"),
        "yfinance_financials": _best_run(results, "yfinance_financials"),
    }
    report_path.write_text(json.dumps(results, indent=2))
    best_path = out_dir / "profile_tuning_best.json"
    best_path.write_text(json.dumps(results["best"], indent=2))
    print(f"Wrote sweep report: {report_path}")


def run_sweep() -> dict[str, Any]:
    out_dir, db_path, report_path = _resolve_paths()
    if db_path.exists():
        db_path.unlink()
    combos = _build_combos()
    inputs = _collect_env_inputs()
    results: dict[str, Any] = {"runs": []}
    for cfg in combos:
        results["runs"].append(_run_single_combo(cfg=cfg, db_path=db_path, inputs=inputs))
    _finalize_reports(results=results, out_dir=out_dir, report_path=report_path)
    if db_path.exists():
        db_path.unlink()
    return results


if __name__ == "__main__":
    run_sweep()
