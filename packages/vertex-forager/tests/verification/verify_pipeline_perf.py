import json
import math
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from vertex_forager.utils import as_dict


def _build_fixture_frame(ticker: str, periods: int = 252) -> object:
    import pandas as pd

    idx = pd.date_range("2024-01-01", periods=periods, freq="B")
    base = {
        "AAPL": 180.0,
        "MSFT": 410.0,
        "NVDA": 850.0,
        "GOOGL": 140.0,
        "AMZN": 170.0,
    }.get(ticker, 100.0)
    rows = []
    for i in range(periods):
        price = base + i * 0.12
        rows.append(
            {
                "Open": price,
                "High": price + 0.8,
                "Low": price - 0.8,
                "Close": price + 0.2,
                "Adj Close": price + 0.2,
                "Volume": 1_000_000 + (i * 1_000),
            }
        )
    frame = pd.DataFrame(rows, index=idx)
    frame.index.name = "Date"
    return frame


def _resolve_duration_s(run: object) -> float:
    duration = getattr(run, "duration_s", None)
    if isinstance(duration, (int, float)) and math.isfinite(duration) and duration > 0:
        return float(duration)
    started_at = getattr(run, "started_at", None)
    finished_at = getattr(run, "finished_at", None)
    if isinstance(started_at, (int, float)) and isinstance(finished_at, (int, float)):
        fallback = float(finished_at) - float(started_at)
        if math.isfinite(fallback) and fallback > 0:
            return fallback
    raise RuntimeError("Invalid benchmark duration_s: expected a finite positive value.")


def _run_mocked_price_collection(
    *,
    client: object,
    tickers: list[str],
    db_path: Path,
    warmup_db_path: Path,
    fixture_map: dict[str, object],
) -> object:
    def _mock_download(*_: object, **kwargs: object) -> object:
        ticker = kwargs.get("tickers")
        if isinstance(ticker, str) and ticker in fixture_map:
            return fixture_map[ticker].copy()
        if isinstance(ticker, str):
            raise ValueError(f"Unexpected ticker requested in mocked yfinance.download: {ticker}")
        raise ValueError("Mocked yfinance.download expected keyword argument `tickers` as a string symbol.")

    class _MockTicker:
        def __init__(self, symbol: str) -> None:
            self._symbol = symbol

        def history(self, **_: object) -> object:
            if self._symbol in fixture_map:
                return fixture_map[self._symbol].copy()
            raise ValueError(f"Unexpected ticker requested in mocked yfinance.Ticker.history: {self._symbol}")

    with patch(
        "vertex_forager.providers.yfinance.fetcher._http_mod.yf",
        new=SimpleNamespace(download=_mock_download, Ticker=_MockTicker),
    ):
        if warmup_db_path.exists():
            warmup_db_path.unlink()
        client.get_price_data(
            tickers=tickers,
            connect_db=warmup_db_path,
            show_progress=False,
        )
        if warmup_db_path.exists():
            warmup_db_path.unlink()
        return client.get_price_data(
            tickers=tickers,
            connect_db=db_path,
            show_progress=False,
        )


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
    out_dir = Path(out_dir_env) if out_dir_env else (Path.cwd() / "output" / "forager-profiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "profile_metrics.json"
    db_path = out_dir / "profile_run.duckdb"
    if db_path.exists():
        db_path.unlink()
    try:
        from vertex_forager.providers.yfinance.client import YFinanceClient
    except ImportError as err:
        if err.name in {"pandas", "yfinance"}:
            print("Skipping verification: install optional deps with `pip install vertex-forager[yfinance]`")
            return
        raise

    client = YFinanceClient(rate_limit=60, metrics_enabled=True, structured_logs=False)
    tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]
    fixture_map = {ticker: _build_fixture_frame(ticker) for ticker in tickers}
    warmup_db_path = out_dir / "profile_run_warmup.duckdb"
    try:
        run = _run_mocked_price_collection(
            client=client,
            tickers=tickers,
            db_path=db_path,
            warmup_db_path=warmup_db_path,
            fixture_map=fixture_map,
        )
    except ImportError as err:
        if err.name in {"pandas", "yfinance"}:
            print("Skipping verification: install optional deps with `pip install vertex-forager[yfinance]`")
            return
        raise
    except Exception as err:
        raise RuntimeError("Benchmark execution failed before metrics could be written.") from err

    data = as_dict(run)
    data["duration_s"] = _resolve_duration_s(run)
    data["started_at"] = getattr(run, "started_at", None)
    data["finished_at"] = getattr(run, "finished_at", None)
    metrics_path.write_text(json.dumps(data, indent=2))
    print(f"Wrote metrics: {metrics_path}")

    if db_path.exists():
        db_path.unlink()
    if warmup_db_path.exists():
        warmup_db_path.unlink()


if __name__ == "__main__":
    main()
