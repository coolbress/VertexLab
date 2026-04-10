# Logging schema

This page documents the structured log fields that `vertex-forager` currently emits through `logging` `extra=` payloads.

Use this reference when building dashboards, alerting rules, or log processors.

## Conventions

- Field names use the `vf_` prefix.
- String identifiers are sanitized before emission.
- Durations are emitted in seconds.
- Counts are emitted as integers.

## Core fields

These fields are the stable core of structured stage logs.

| Field | Type | Meaning |
| --- | --- | --- |
| `vf_provider` | `str` | Provider identifier such as `yfinance` or `sharadar`. |
| `vf_dataset` | `str` | Provider-local dataset name. |
| `vf_symbol` | `str` | Single symbol, `*`, or sanitized symbol placeholder. |
| `vf_stage` | `str` | Event/stage name. |
| `vf_attempt` | `int` | Attempt counter for the event. |
| `vf_duration_s` | `float \| None` | Duration in seconds when known. |

## Event categories

### Client run start

Source: [BaseClient.run_pipeline](file:///Users/coolbress/vertex-lab/packages/vertex-forager/src/vertex_forager/clients/base.py)

Guaranteed fields:

- `vf_provider: str`
- `vf_dataset: str`
- `vf_symbol: "*"`
- `vf_stage: "client_run_start"`
- `vf_symbols: int`
- `vf_attempt: int`
- `vf_duration_s: 0.0`

### Client run complete

Source: [BaseClient.run_pipeline](file:///Users/coolbress/vertex-lab/packages/vertex-forager/src/vertex_forager/clients/base.py)

Guaranteed fields:

- `vf_provider: str`
- `vf_dataset: str`
- `vf_symbol: "*"`
- `vf_stage: "client_run_end"`
- `vf_errors: int`
- `vf_attempt: int`
- `vf_duration_s: float`

### Fetch and pipeline stage events

Source: [VertexForager._log_structured](file:///Users/coolbress/vertex-lab/packages/vertex-forager/src/vertex_forager/core/pipeline.py)

Guaranteed fields:

- `vf_provider: str`
- `vf_dataset: str`
- `vf_symbol: str`
- `vf_stage: str`
- `vf_attempt: int`
- `vf_duration_s: float | None`

Typical stage values include fetch start, fetch completion, parse completion, write completion, and error stages. The exact stage value is part of the event payload and should be matched as a string rather than inferred from the logger name.

### Router packet completion

Source: [YFinanceRouter._log_structured](file:///Users/coolbress/vertex-lab/packages/vertex-forager/src/vertex_forager/providers/yfinance/router.py)

Guaranteed fields:

- `vf_provider: str`
- `vf_dataset: str`
- `vf_symbol: str`
- `vf_stage: str`
- `vf_attempt: int`
- `vf_duration_s: float`
- `vf_packets: int`

Optional fields:

- `vf_rows: int`

### Pipeline summary

Source: [emit_pipeline_summary](file:///Users/coolbress/vertex-lab/packages/vertex-forager/src/vertex_forager/core/lifecycle.py)

Guaranteed fields:

- `vf_provider: str`
- `vf_dataset: str`
- `vf_symbol: "*"`
- `vf_stage: "pipeline_summary"`
- `vf_attempt: 0`
- `vf_duration_s: float`

Additional summary metrics are emitted as dynamic integer fields with the prefix `vf_`, for example:

- `vf_jobs_completed`
- `vf_rows_written`
- `vf_errors`
- `vf_dlq_spooled_total`

Treat these as summary counters rather than per-record dimensions.

## Write and DLQ events

Dedicated structured write-success, write-error, DLQ enqueue, and DLQ drain events are not emitted as separate logger schemas today.

Current operator guidance:

- use pipeline stage logs for per-attempt write/fetch progress
- use pipeline summary counters for final write/DLQ totals
- use `RunResult.errors` and DLQ state for detailed failure inspection

If new dedicated write or DLQ structured events are added later, they should extend this page with field-level guarantees before shipping.
