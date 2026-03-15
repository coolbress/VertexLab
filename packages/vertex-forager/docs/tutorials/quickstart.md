# Quickstart

Follow this hands‑on tutorial to fetch data and persist it to DuckDB.

## Prerequisites

- Python 3.10+
- Install package (from GitHub, until PyPI release):

```bash
# Latest main (pin to a tag/commit for reproducibility)
uv pip install "git+https://github.com/coolbress/vertex-lab@main#subdirectory=packages/vertex-forager"

# Example: pin to a version tag
uv pip install "git+https://github.com/coolbress/vertex-lab@v0.1.0#subdirectory=packages/vertex-forager"
```

## Provider‑agnostic: Fetch and Persist (sync‑friendly)

```python
import os
from vertex_forager import create_client

provider = os.getenv("VF_PROVIDER", "yfinance").strip()
kwargs = {}
if provider == "sharadar":
    kwargs["api_key"] = os.environ["SHARADAR_API_KEY"]
client = create_client(provider=provider, **kwargs)
tickers = [t.strip() for t in (os.getenv("VF_TICKERS") or "AAPL,MSFT").split(",")]

# jupyter_safe wrapper lets you call this like a normal (sync) function
df = client.get_price_data(tickers=tickers, show_progress=False)
print(df)
```

## Run Examples

- Minimal (env‑driven)

```bash
VF_PROVIDER=yfinance VF_TICKERS=AAPL,MSFT \
uv run python packages/vertex-forager/examples/minimal_inmemory.py
```

- Sharadar (requires SHARADAR_API_KEY)

```bash
export SHARADAR_API_KEY=...   # your key
VF_PROVIDER=sharadar VF_TICKERS=AAPL,MSFT \
uv run python packages/vertex-forager/examples/minimal_inmemory.py
```

## CLI Quick Commands

- Status

```bash
uv run vertex-forager status
```

- Collect (Sharadar)

```bash
export SHARADAR_API_KEY=...
uv run vertex-forager collect -s AAPL -s MSFT --source sharadar
```

## Next Steps

- Configure concurrency and retries: see [EngineConfig](../reference/config.md)
- Operate without DLQ spooling: see [DLQ disabled](../how-to/dlq-disabled.md)
- Tune chunked flush: see [Chunked flush tuning](../how-to/chunked-flush.md)
