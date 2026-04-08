# Configure Retry

Use this guide when you want to tune how vertex-forager retries transient failures.

## Start with RetryConfig

```python
from vertex_forager import RetryConfig, create_client

client = create_client(
    provider="sharadar",
    api_key="YOUR_SHARADAR_API_KEY",
    rate_limit=300,
    retry=RetryConfig(
        max_attempts=4,
        base_backoff_s=0.5,
        max_backoff_s=30.0,
        backoff_mode="full_jitter",
    ),
)
```

## Know what each field changes

### `max_attempts`

Controls how many total tries vertex-forager will make for retryable work.

Increase it when:

- the provider is flaky but eventually recovers
- you prefer eventual success over a faster failure

Keep it lower when:

- repeated failures usually indicate a bad request
- you want faster feedback to operators

### `base_backoff_s`

Sets the starting delay before retry growth begins.

Increase it when:

- providers punish rapid retries
- error bursts correlate with traffic spikes

### `max_backoff_s`

Caps the largest backoff delay.

Use it to prevent runaway retry sleeps.

### `backoff_mode`

Choose one of:

- `full_jitter`
- `equal`

Use `full_jitter` when:

- you want the best spreading under contention
- many similar requests may fail together

Use `equal` when:

- you want less variance between attempts
- you still want randomness but with a higher floor

### `retry_status_codes`

Extends or narrows the HTTP statuses treated as retryable.

Adjust this when:

- a provider uses non-standard retryable statuses
- you know certain statuses should fail fast instead

## Pick a practical policy

### Conservative policy

```python
RetryConfig(
    max_attempts=3,
    base_backoff_s=0.5,
    max_backoff_s=10.0,
    backoff_mode="full_jitter",
)
```

Use this for routine collection.

### More patient policy

```python
RetryConfig(
    max_attempts=5,
    base_backoff_s=1.0,
    max_backoff_s=60.0,
    backoff_mode="full_jitter",
)
```

Use this for longer runs where transient provider instability is common.

## Combine retry with throttling

Retry alone does not reduce steady-state pressure. If the provider begins throttling because your request rate is too high, pair retry tuning with `AdaptiveThrottleConfig(...)`.

## Next steps

- Understand the retry mechanism itself: [How retry and backoff work](../explanation/how-retry-and-backoff-work.md)
- Tune concurrency and pacing too: [How flow controller works](../explanation/how-flow-controller-works.md)
- Review exact field defaults: [Configuration](../reference/config.md)
