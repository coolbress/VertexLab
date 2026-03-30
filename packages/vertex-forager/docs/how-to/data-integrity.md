# Data integrity controls (opt-in)

Enable stricter validation and lightweight dedup/upsert without changing defaults.

## Strict schema validation

- Default: lenient — missing columns are added with `null`, casts use `strict=False`.
- Strict: set `SchemaMapper(strict_validation=True)` to fail fast on invalid inputs.

```python
from vertex_forager.schema.mapper import SchemaMapper

mapper = SchemaMapper(strict_validation=False)  # default
strict_mapper = SchemaMapper(strict_validation=True)  # raises on missing columns / strict casts
```

Counters (metrics enabled only):

- `schema_missing_cols_filled`: cells filled as `null` in non‑strict mode
- `schema_extra_cols_preserved_count`: extra input columns preserved

## In‑memory dedup/upsert by unique key

Use an optional unique key to deduplicate buffered rows when collecting results in memory.

Behavior:

- Frames are concatenated, then deduplicated by `unique_key` with `keep="last"`, `maintain_order=True`.
- Sorting (if requested) is applied after dedup/upsert.

```python
from vertex_forager.writers.memory import InMemoryBufferWriter

writer = InMemoryBufferWriter()
writer.set_unique_key(["provider", "ticker", "date"])
df = writer.collect_table("yfinance_price")
```

If collecting via a provider client, the schema’s `unique_key` is automatically passed to the in‑memory writer when available.

Counter (metrics enabled only):

- `inmem_dedup_dropped_rows`: number of rows removed by dedup/upsert

## Notes

- Metrics gating: counters are merged into `RunResult.metrics_counters` only when `create_client(..., metrics_enabled=True)` is used.
- Defaults are unchanged unless you opt in to these features.

## Data Quality Rules

`TableSchema` supports a `quality_rules` tuple so you can attach lightweight validation to a table without changing the main fetch and write flow.

### Available rules

- `DataQualityRule`
  - Protocol for custom validators. Implement `validate(df) -> list[str]` and return violation messages.
- `NoDuplicateRows`
  - Detects duplicate rows either across the full frame or within a subset of columns such as `["ticker", "date"]`.
- `NoFutureDates`
  - Flags rows whose date or datetime columns are later than the current time.
- `NoNegativePrices`
  - Flags negative numeric values in common price columns such as `open`, `high`, `low`, and `close`.

### Pass rules through a schema

```python
import polars as pl

from vertex_forager import NoDuplicateRows, NoFutureDates, NoNegativePrices
from vertex_forager.schema.config import TableSchema

price_schema = TableSchema(
    table="custom_price",
    schema={"ticker": pl.String, "date": pl.Date, "close": pl.Float64},
    unique_key=("ticker", "date"),
    quality_rules=(
        NoDuplicateRows(subset=["ticker", "date"]),
        NoFutureDates(date_columns=["date"]),
        NoNegativePrices(price_columns=["close"]),
    ),
)
```

### When to use each rule

- Use `NoDuplicateRows` when a table has a natural key and duplicate packets should be surfaced before downstream analysis.
- Use `NoFutureDates` when provider timestamps must never exceed the observation time for the run.
- Use `NoNegativePrices` when market data should always remain non-negative and you want quick anomaly detection.

Violations are logged, aggregated into `RunResult.quality_violations`, and kept separate from schema-mapping failures so you can decide whether to treat them as warnings or operational alerts.
