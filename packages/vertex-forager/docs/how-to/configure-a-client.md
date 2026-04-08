# Configure a Client

Use this guide when you want to choose the right `create_client(...)` setup for a real workload instead of relying on defaults blindly.

## Start with the minimum setup

### YFinance

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
```

Use this when:

- you do not need an API key
- provider defaults are acceptable
- you want the fastest path to a first run

### Sharadar

```python
from vertex_forager import create_client

client = create_client(
    provider="sharadar",
    api_key="YOUR_SHARADAR_API_KEY",
    rate_limit=300,
)
```

Use this when:

- you have a Sharadar key
- you want explicit control over the request budget
- you plan to persist larger runs and want predictable pacing

## Know which inputs are required

### Always required

- `provider`

### Required only for Sharadar

- `api_key`
- `rate_limit`

### Common optional inputs

- `quality_check`
- `concurrency`
- `schedule`
- `retry`
- `throttle`
- `limits`
- `storage`

## Choose grouped config only when you need it

### RetryConfig

Use `RetryConfig(...)` when:

- your provider or network returns transient failures
- you need more or fewer retry attempts
- you want to change backoff timing or retry status codes

### AdaptiveThrottleConfig

Use `AdaptiveThrottleConfig(...)` when:

- the provider reacts poorly to burst traffic
- you want the effective RPM to downshift automatically after errors
- you need smoother recovery after a bad window

### HTTPConfig

Use `HTTPConfig(...)` when:

- the default timeout is too low or too high
- you want to cap total connections
- you need more keepalive capacity for repeated runs

### StorageConfig

Use `StorageConfig(...)` when:

- DuckDB flushes too often or not often enough
- you want a longer or shorter checkpoint retention window
- you want to keep run history longer for operations or audits

## Pick a configuration by workload

### First exploration

```python
from vertex_forager import create_client

client = create_client(provider="yfinance")
```

Choose this for:

- notebook exploration
- in-memory fetches
- one-off checks

### Bulk persisted collection

```python
from vertex_forager import HTTPConfig, RetryConfig, StorageConfig, create_client

client = create_client(
    provider="sharadar",
    api_key="YOUR_SHARADAR_API_KEY",
    rate_limit=300,
    concurrency=4,
    retry=RetryConfig(max_attempts=4),
    limits=HTTPConfig(max_connections=100, max_keepalive_connections=50, timeout_s=30.0),
    storage=StorageConfig(flush_threshold_rows=500_000),
)
```

Choose this for:

- larger DuckDB writes
- long-running collections
- workloads where retry and write behavior matter

### Strict quality enforcement

```python
from vertex_forager import create_client

client = create_client(
    provider="yfinance",
    quality_check="error",
)
```

Choose this when:

- quality violations should stop the run immediately
- you prefer fail-fast behavior over partial success

## What not to configure from the public surface

The stable public API intentionally does not expose internal engine-style knobs as first-class workflow controls. Stay with the grouped config objects and top-level options above instead of searching for hidden flags.

## Next steps

- Run a real collection: [Collect data](collect-data.md)
- Tune retry policy: [Configure retry](configure-retry.md)
- Learn the exact option inventory: [Configuration](../reference/config.md)
