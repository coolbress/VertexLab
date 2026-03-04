import os
import logging
import json
from pathlib import Path

from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.providers.sharadar.client import SharadarClient
from vertex_forager.utils import as_dict, load_tickers_env

logger = logging.getLogger(__name__)


def main() -> None:
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
    os.environ.setdefault("VF_METRICS_ENABLED", "1")

    # ---------- YFinance Financials ----------
    yf_tickers = load_tickers_env(
        "YF_TICKERS",
        ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "NFLX", "ADBE", "CSCO"],
    )
    yfc = YFinanceClient(rate_limit=60, structured_logs=False)
    yf_run = yfc.get_financials(
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
            sh_run = shc.get_fundamental_data(
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

    if db_path.exists():
        db_path.unlink()


if __name__ == "__main__":
    main()
