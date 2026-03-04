import os
import json
from pathlib import Path
from typing import Any

from vertex_forager.providers.yfinance.client import YFinanceClient


def main() -> None:
    out_dir_env = os.getenv("VF_PROFILE_OUTPUT_DIR")
    out_dir = Path(out_dir_env) if out_dir_env else (Path.cwd() / "output" / "forager-profiles")
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

    def _as_dict(obj: Any) -> dict[str, Any]:
        return {
            "counters": getattr(obj, "metrics_counters", {}),
            "histograms": getattr(obj, "metrics_histograms", {}),
            "summary": getattr(obj, "metrics_summary", {}),
            "tables": getattr(obj, "tables", {}),
            "errors": getattr(obj, "errors", []),
        }

    data = _as_dict(run)
    metrics_path.write_text(json.dumps(data, indent=2))
    print(f"Wrote metrics: {metrics_path}")

    if db_path.exists():
        db_path.unlink()


if __name__ == "__main__":
    main()
