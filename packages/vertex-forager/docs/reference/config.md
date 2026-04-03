# Configuration Reference

Public runtime configuration is now centered on `create_client(...)` plus grouped config models.

## Required Inputs

- `provider: str` — provider identifier such as `"sharadar"` or `"yfinance"`
- `api_key: str | None` — required for Sharadar, unused for YFinance
- `rate_limit: int` — requests per minute

## Top-Level Client Parameters

- `schedule: SchedulerConfig = SchedulerConfig()`
- `concurrency: int | None = None`

Stage logs are always emitted at DEBUG level; the host application controls visibility and formatting via standard `logging` configuration.

## Grouped Public Config

### RetryConfig

- `max_attempts: int`
- `base_backoff_s: float`
- `max_backoff_s: float` — must be `>= base_backoff_s`
- `backoff_mode: Literal["full_jitter", "equal"]`
- `retry_status_codes: tuple[int, ...]`

### AdaptiveThrottleConfig

- `enabled: bool`
- `window_s: int`
- `error_rate_threshold: float` — in `[0, 1]`
- `rpm_floor_ratio: float` — in `[0, 1]`
- `recovery_factor: float` — in `[0, 1]`
- `healthy_window_s: int`

### HTTPConfig

- `max_connections: int`
- `max_keepalive_connections: int`
- `timeout_s: float` — HTTP request timeout in seconds

### SchedulerConfig

- `quantum: int = 3`
- `max_pending_per_symbol: int | None = None`
- `backpressure_threshold: int | None = None`

### StorageConfig

- `flush_threshold_rows: int` — DuckDB write buffer threshold before flush begins
- `checkpoint_retention_days: int = 7` — retention window for completed checkpoint state
- `run_history_retention_days: int = 90` — retention window for run-history records
- `dlq_tmp_retention_s: int` — retention window for DLQ `.tmp` artefacts

DLQ spooling, periodic cleanup, writer chunking, writer worker count, and memory guard thresholds are internal defaults and are no longer public tuning knobs.

## Example

```python
from vertex_forager import (
    AdaptiveThrottleConfig,
    HTTPConfig,
    RetryConfig,
    SchedulerConfig,
    StorageConfig,
    create_client,
)

client = create_client(
    provider="sharadar",
    api_key="...",
    rate_limit=300,
    concurrency=4,
    schedule=SchedulerConfig(
        quantum=3,
        max_pending_per_symbol=50,
        backpressure_threshold=120,
    ),
    retry=RetryConfig(max_attempts=3),
    throttle=AdaptiveThrottleConfig(rpm_floor_ratio=1.0),
    limits=HTTPConfig(max_connections=200, max_keepalive_connections=100, timeout_s=30.0),
    storage=StorageConfig(
        flush_threshold_rows=500_000,
        checkpoint_retention_days=7,
        run_history_retention_days=90,
    ),
)
```
