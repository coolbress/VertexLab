import os
from pathlib import Path
from typing import Sequence

from vertex_forager import create_client


def _tickers_from_env(default: Sequence[str] = ("AAPL", "MSFT")) -> list[str]:
    raw = os.getenv("VF_TICKERS")
    if not raw:
        return list(default)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or list(default)


def _db_path() -> Path:
    raw = os.getenv("VF_DUCKDB_PATH")
    return Path(raw) if raw else Path("./forager.duckdb")


def main() -> None:
    provider = os.getenv("VF_PROVIDER", "yfinance").strip()
    kwargs = {}
    if provider == "sharadar":
        kwargs["api_key"] = os.environ["SHARADAR_API_KEY"]
    client = create_client(provider=provider, metrics_enabled=True, **kwargs)

    tickers = _tickers_from_env()
    db = _db_path()
    uri = f"duckdb://{db}"
    run = client.get_price_data(tickers=tickers, connect_db=uri, show_progress=False)
    print(getattr(run, "tables", run))
    if not db.exists():
        raise SystemExit("Expected DuckDB file was not created")
    print(f"Wrote to {db}")


if __name__ == "__main__":
    main()
