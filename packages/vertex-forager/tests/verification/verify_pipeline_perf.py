from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import time
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
        "vertex_forager.core.http.yf",
        new=SimpleNamespace(download=_mock_download, Ticker=_MockTicker),
    ):
        if warmup_db_path.exists():
            warmup_db_path.unlink()
        client.get_price_data(
            tickers=tickers,
            connect_db=warmup_db_path,
            progress=False,
        )
        if warmup_db_path.exists():
            warmup_db_path.unlink()
        return client.get_price_data(
            tickers=tickers,
            connect_db=db_path,
            progress=False,
        )


def _measure_domain_model_construction() -> dict[str, float]:
    import polars as pl
    from pydantic import BaseModel, ConfigDict

    from vertex_forager.core.config import FetchJob, FramePacket, RequestSpec

    class _PydanticFetchJob(BaseModel):
        provider: str
        dataset: str
        symbol: str | None = None
        spec: RequestSpec
        context: dict[str, object] = {}

    class _PydanticFramePacket(BaseModel):
        provider: str
        table: str
        frame: pl.DataFrame
        observed_at: object
        partition_date: object = None
        context: dict[str, object] = {}

        model_config = ConfigDict(arbitrary_types_allowed=True)

    request_spec = RequestSpec(url="https://example.test", params={"page": 1, "symbols": ["AAPL", "MSFT"]})
    frame = pl.DataFrame({"ticker": ["AAPL"], "close": [181.2]})
    observed_at = datetime.now(timezone.utc)
    iterations = 20_000

    start = time.perf_counter()
    for _ in range(iterations):
        FetchJob(
            provider="yfinance",
            dataset="price",
            symbol="AAPL",
            spec=request_spec,
            context={"attempt": 1, "trace_id": "trace-1"},
        )
    fetch_job_current_s = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(iterations):
        _PydanticFetchJob(
            provider="yfinance",
            dataset="price",
            symbol="AAPL",
            spec=request_spec,
            context={"attempt": 1, "trace_id": "trace-1"},
        )
    fetch_job_pydantic_s = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(iterations):
        FramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=frame,
            observed_at=observed_at,
            context={"ticker": "AAPL"},
        )
    frame_packet_current_s = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(iterations):
        _PydanticFramePacket(
            provider="yfinance",
            table="yfinance_price",
            frame=frame,
            observed_at=observed_at,
            context={"ticker": "AAPL"},
        )
    frame_packet_pydantic_s = time.perf_counter() - start

    return {
        "iterations": float(iterations),
        "fetch_job_current_avg_us": (fetch_job_current_s / iterations) * 1_000_000.0,
        "fetch_job_pydantic_avg_us": (fetch_job_pydantic_s / iterations) * 1_000_000.0,
        "fetch_job_speedup_x": fetch_job_pydantic_s / fetch_job_current_s if fetch_job_current_s > 0 else 0.0,
        "frame_packet_current_avg_us": (frame_packet_current_s / iterations) * 1_000_000.0,
        "frame_packet_pydantic_avg_us": (frame_packet_pydantic_s / iterations) * 1_000_000.0,
        "frame_packet_speedup_x": frame_packet_pydantic_s / frame_packet_current_s
        if frame_packet_current_s > 0
        else 0.0,
        "baseline": "local_pydantic_baseline",
    }


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

    client = YFinanceClient(rate_limit=60)
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
    data["construction_benchmarks"] = _measure_domain_model_construction()
    metrics_path.write_text(json.dumps(data, indent=2))
    print(f"Wrote metrics: {metrics_path}")

    if db_path.exists():
        db_path.unlink()
    if warmup_db_path.exists():
        warmup_db_path.unlink()


if __name__ == "__main__":
    main()
