import json
import os
from pathlib import Path
import time
from typing import Any

from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.utils import load_tickers_env, set_env


def run_sweep() -> dict[str, Any]:
    out_dir_env = os.getenv("VF_PROFILE_OUTPUT_DIR")
    out_dir = Path(out_dir_env) if out_dir_env else (
        Path.cwd() / "output" / "forager-profiles"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "profile_sweep.duckdb"
    report_path = out_dir / "profile_sweep_results.json"
    if db_path.exists():
        db_path.unlink()
    os.environ.setdefault("VF_METRICS_ENABLED", "1")

    # Config combos (kept minimal to avoid long runs)
    combos: list[dict[str, Any]] = [
        {
            "VF_CONCURRENCY": 8,
            "VF_FLUSH_THRESHOLD_ROWS": 100_000,
            "VF_HTTP_MAX_KEEPALIVE": 100,
            "VF_HTTP_MAX_CONNECTIONS": 200,
            "VF_HTTP_TIMEOUT_S": 30,
        },
        {
            "VF_CONCURRENCY": 12,
            "VF_FLUSH_THRESHOLD_ROWS": 150_000,
            "VF_HTTP_MAX_KEEPALIVE": 150,
            "VF_HTTP_MAX_CONNECTIONS": 300,
            "VF_HTTP_TIMEOUT_S": 30,
        },
        {
            "VF_CONCURRENCY": 16,
            "VF_FLUSH_THRESHOLD_ROWS": 200_000,
            "VF_HTTP_MAX_KEEPALIVE": 200,
            "VF_HTTP_MAX_CONNECTIONS": 400,
            "VF_HTTP_TIMEOUT_S": 45,
        },
        {
            "VF_CONCURRENCY": 20,
            "VF_FLUSH_THRESHOLD_ROWS": 250_000,
            "VF_HTTP_MAX_KEEPALIVE": 200,
            "VF_HTTP_MAX_CONNECTIONS": 400,
            "VF_HTTP_TIMEOUT_S": 45,
        },
        {
            "VF_CONCURRENCY": 24,
            "VF_FLUSH_THRESHOLD_ROWS": 300_000,
            "VF_HTTP_MAX_KEEPALIVE": 250,
            "VF_HTTP_MAX_CONNECTIONS": 500,
            "VF_HTTP_TIMEOUT_S": 45,
        },
    ]

    yf_tickers_price = load_tickers_env(
        "YF_TICKERS_PRICE", ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]
    )
    yf_tickers_fin = load_tickers_env("YF_TICKERS_FIN", ["AAPL", "MSFT", "NVDA"])
    yf_start = os.getenv("YF_PRICE_START_DATE")
    yf_end = os.getenv("YF_PRICE_END_DATE")
    sh_key = os.getenv("SHARADAR_API_KEY")
    sh_tickers = load_tickers_env("SH_TICKERS", ["AAPL", "MSFT", "NVDA"])
    sh_start = os.getenv("SH_START_DATE")
    sh_end = os.getenv("SH_END_DATE")

    results: dict[str, Any] = {"runs": []}

    original_env = os.environ.copy()
    for _i, cfg in enumerate(combos):
        try:
            set_env(cfg)
            run_entry: dict[str, Any] = {"env": dict(cfg), "measurements": {}}

            # YFinance price
            yfc = YFinanceClient(rate_limit=60, structured_logs=False)
            t0 = time.monotonic()
            yf_price = yfc.get_price_data(
                tickers=yf_tickers_price,
                connect_db=db_path,
                show_progress=False,
                start_date=yf_start,
                end_date=yf_end,
            )
            t1 = time.monotonic()
            run_entry["measurements"]["yfinance_price"] = {
                "duration_s": round(t1 - t0, 3),
                "metrics": {
                    "summary": getattr(yf_price, "metrics_summary", {}),
                    "counters": getattr(yf_price, "metrics_counters", {}),
                },
            }

            # YFinance financials (annual income_stmt)
            yfc2 = YFinanceClient(rate_limit=60, structured_logs=False)
            t2 = time.monotonic()
            yf_fin = yfc2.get_financials(
                kind="income_stmt",
                period="annual",
                tickers=yf_tickers_fin,
                connect_db=db_path,
                show_progress=False,
            )
            t3 = time.monotonic()
            run_entry["measurements"]["yfinance_financials"] = {
                "duration_s": round(t3 - t2, 3),
                "metrics": {
                    "summary": getattr(yf_fin, "metrics_summary", {}),
                    "counters": getattr(yf_fin, "metrics_counters", {}),
                },
            }

            # Optional: Sharadar fundamental MRT if key present
            if sh_key:
                try:
                    shc = SharadarClient(
                        api_key=sh_key, rate_limit=60, structured_logs=False
                    )
                    t4 = time.monotonic()
                    sh_fin = shc.get_fundamental_data(
                        tickers=sh_tickers,
                        connect_db=db_path,
                        dimension="MRT",
                        start_date=sh_start,
                        end_date=sh_end,
                    )
                    t5 = time.monotonic()
                    run_entry["measurements"]["sharadar_sf1_mrt"] = {
                        "duration_s": round(t5 - t4, 3),
                        "metrics": {
                            "summary": getattr(sh_fin, "metrics_summary", {}),
                            "counters": getattr(sh_fin, "metrics_counters", {}),
                        },
                    }
                except Exception as e:
                    print(f"Sharadar verification skipped due to error: {e}")
                    run_entry["measurements"]["sharadar_sf1_mrt"] = {"error": str(e)}

            results["runs"].append(run_entry)
        finally:
            os.environ.clear()
            os.environ.update(original_env)

    # Write report with simple best selection by shortest durations
    def _best(run_key: str) -> dict[str, Any]:
        ranked = sorted(
            results["runs"],
            key=lambda r: r["measurements"].get(run_key, {}).get(
                "duration_s", float("inf")
            ),
        )
        return ranked[0] if ranked else {}
    results["best"] = {
        "yfinance_price": _best("yfinance_price"),
        "yfinance_financials": _best("yfinance_financials"),
    }
    report_path.write_text(json.dumps(results, indent=2))
    best_path = out_dir / "profile_tuning_best.json"
    best_path.write_text(json.dumps(results["best"], indent=2))
    print(f"Wrote sweep report: {report_path}")

    # Cleanup db
    if db_path.exists():
        db_path.unlink()

    return results


if __name__ == "__main__":
    run_sweep()
