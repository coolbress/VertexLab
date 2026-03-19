from collections.abc import Sequence
import os

from vertex_forager import create_client


def _env_list(name: str, default: Sequence[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts or list(default)


def main() -> None:
    provider = os.getenv("VF_PROVIDER", "yfinance").strip()
    tickers = _env_list("VF_TICKERS", ("AAPL", "MSFT"))
    kwargs = {}
    if provider == "sharadar":
        kwargs["api_key"] = os.environ["SHARADAR_API_KEY"]
    client = create_client(provider=provider, **kwargs)
    # jupyter_safe wrapper allows sync usage of async methods
    df = client.get_price_data(tickers=tickers, show_progress=False)
    try:
        print(df.head())  # type: ignore[attr-defined]
    except Exception:
        print(df)


if __name__ == "__main__":
    main()
