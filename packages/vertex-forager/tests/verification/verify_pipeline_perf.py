import json
import os
from pathlib import Path

from vertex_forager.providers.yfinance.client import YFinanceClient
from vertex_forager.utils import as_dict


def main() -> None:
    """Verify pipeline performance for Price data.

    Executes a price data collection run for standard tickers and writes
    performance metrics to 'profile_metrics.json' in the configured output
    directory.

    Side Effects:
        - Creates/deletes 'profile_run.duckdb' in VF_PROFILE_OUTPUT_DIR.
        - Writes 'profile_metrics.json' to VF_PROFILE_OUTPUT_DIR.
    """
    out_dir_env = os.getenv("VF_PROFILE_OUTPUT_DIR")
    out_dir = (
        Path(out_dir_env)
        if out_dir_env
        else (Path.cwd() / "output" / "forager-profiles")
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "profile_metrics.json"
    db_path = out_dir / "profile_run.duckdb"
    if db_path.exists():
        db_path.unlink()

    client = YFinanceClient(rate_limit=60, metrics_enabled=True, structured_logs=False)
    tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

    run = client.get_price_data(
        tickers=tickers,
        connect_db=db_path,
        show_progress=False,
    )

    data = as_dict(run)
    metrics_path.write_text(json.dumps(data, indent=2))
    print(f"Wrote metrics: {metrics_path}")

    if db_path.exists():
        db_path.unlink()


if __name__ == "__main__":
    main()
