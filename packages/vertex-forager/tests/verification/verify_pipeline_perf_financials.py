import os
import logging
import json
import asyncio
from pathlib import Path

from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.utils import as_dict, load_tickers_env

logger = logging.getLogger(__name__)


async def main_async() -> None:
    """Verify pipeline performance for Financials and Sharadar data.

    Executes a financials data collection run (YFinance Income Stmt and optional Sharadar MRT)
    and writes performance metrics to 'profile_financials_metrics.json'.
    
    Env Vars:
        VF_PROFILE_OUTPUT_DIR: Directory for output files (default: output/forager-profiles).
        VF_METRICS_ENABLED: Enabled by default ("1").
        YF_TICKERS: Comma-separated tickers for YFinance (default: top 10 tech).
        SHARADAR_API_KEY: If present, runs Sharadar verification.

    Side Effects:
        - Creates/deletes 'profile_financials.duckdb' in output dir.
        - Writes 'profile_financials_metrics.json'.
    """
    out_dir_env = os.getenv("VF_PROFILE_OUTPUT_DIR")
    out_dir = Path(out_dir_env) if out_dir_env else (Path.cwd() / "output" / "forager-profiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "profile_financials_metrics.json"
    db_path = out_dir / "profile_financials.duckdb"
    if db_path.exists():
        db_path.unlink()

    # Apply environment-driven tuning via BaseClient (VF_CONCURRENCY/VF_FLUSH_THRESHOLD_ROWS)
    prev_vf_metrics = os.environ.get("VF_METRICS_ENABLED")
    os.environ.setdefault("VF_METRICS_ENABLED", "1")

    try:
        # ---------- YFinance Financials ----------
        yf_tickers = load_tickers_env(
            "YF_TICKERS",
            ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "NFLX", "ADBE", "CSCO"],
        )
        yfc = YFinanceClient(rate_limit=60, structured_logs=False)
        yf_run = await yfc.get_financials(
            kind="income_stmt",
            period="annual",
            tickers=yf_tickers,
            connect_db=db_path,
            show_progress=False,
        )

        # ---------- Optional: Sharadar (requires SHARADAR_API_KEY) ----------
        sh_key = os.getenv("SHARADAR_API_KEY")
        sh_run = None
        sh_error: str | None = None
        if sh_key:
            try:
                shc = SharadarClient(api_key=sh_key, rate_limit=60, structured_logs=False)
                sh_run = await shc.get_fundamental_data(
                    tickers=yf_tickers[:5],
                    connect_db=db_path,
                    dimension="MRT",
                )
            except Exception as e:
                logger.warning(f"Sharadar verification skipped due to error: {e}", exc_info=True)
                sh_run = None
                sh_error = str(e)

        data = {
            "yfinance_financials": as_dict(yf_run),
            "sharadar_sf1_optional": as_dict(sh_run),
            "sharadar_sf1_optional_error": sh_error,
        }
        metrics_path.write_text(json.dumps(data, indent=2))
        print(f"Wrote metrics: {metrics_path}")

    finally:
        # Restore environment
        if prev_vf_metrics is None:
            os.environ.pop("VF_METRICS_ENABLED", None)
        else:
            os.environ["VF_METRICS_ENABLED"] = prev_vf_metrics
            
        # Cleanup DB
        if db_path.exists():
            try:
                db_path.unlink()
            except OSError as e:
                logger.warning(f"Failed to cleanup temp DB {db_path}: {e}")


def main() -> None:
    """Run the financials performance verification pipeline.

    Entry point that wraps main_async() for synchronous execution.
    See main_async() for full environment variable configuration and side effects.
    """
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
