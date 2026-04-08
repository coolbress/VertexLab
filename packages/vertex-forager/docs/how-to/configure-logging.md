# Configure Logging

Use this guide when you want to surface structured logs from vertex-forager in your own application.

## Enable Python logging

```python
import logging

from vertex_forager import create_client

logging.basicConfig(level=logging.INFO)

client = create_client(provider="yfinance")
```

This is enough to see higher-level messages if your application enables the package logger.

## Turn on DEBUG for stage-level events

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("vertex_forager").setLevel(logging.DEBUG)
```

Use DEBUG when you want to inspect stage transitions such as:

- `http_start`
- `http_end`
- `write_flush`
- `dlq_spooled`

## Filter the package logger only

```python
import logging

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

logger = logging.getLogger("vertex_forager")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
```

Use this when you do not want the rest of your application to become noisy.

## Filter for a subset of events

If your host logging stack supports filters, filter by the structured event name or stage field and keep only the categories you care about:

- HTTP stages when debugging provider behavior
- write stages when debugging DuckDB persistence
- DLQ stages when debugging flush failures and replay

## What to watch during a run

### Transport visibility

Look for:

- request start/end
- retry reasons
- latency changes

### Persistence visibility

Look for:

- flush starts
- rows written
- DLQ spool events
- replay outcomes

## Use logs together with RunResult

Logs tell you what happened during the run. `RunResult` tells you the final outcome. Use both together:

- logs for timing and failure sequence
- `RunResult.errors` for run-level failures
- `RunResult.dlq_counts` for replay vs pending state

## Next steps

- Understand the pipeline events behind the logs: [How pipeline orchestrates](../explanation/how-pipeline-orchestrates.md)
- Understand retry events: [How retry and backoff work](../explanation/how-retry-and-backoff-work.md)
