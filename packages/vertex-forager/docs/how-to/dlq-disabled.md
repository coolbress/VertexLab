# Operate with DLQ Disabled

Disable on‑disk DLQ spooling when local persistence is not allowed, while still summarizing failures and attempting per‑packet rescue.

## Goal

- Skip writing IPC files to the DLQ directory.
- Record `DLQ=disabled; rescued=N; remaining=M` in RunResult summaries.
- Keep per‑table counts in `RunResult.dlq_counts`.

## Steps

1) Configure (apply via client creation):

```python
import os
from vertex_forager import create_client

client = create_client(
    provider=os.getenv("VF_PROVIDER", "yfinance").strip(),
    rate_limit=60,
    dlq_enabled=False,
    # optional: metrics/logs
    metrics_enabled=True,          # emit counters/histograms
    structured_logs=True,          # emit structured stages
)
```

2) Run your fetch:

```python
res = client.get_price_data(tickers=["AAPL"], connect_db="duckdb://./forager.duckdb")
print(res.errors)        # includes “DLQ=disabled…”
print(res.dlq_counts)    # per-table rescued/remaining
```

## Notes

- Structured logs require `structured_logs=True`. Without it, the RunResult summaries and counts still populate.
- Metrics counters (`dlq_rescued_total`, `dlq_remaining_total` etc.) require `metrics_enabled=True`.
