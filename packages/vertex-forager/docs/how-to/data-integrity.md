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

- Metrics gating: counters are merged into `RunResult.metrics_counters` only when `EngineConfig.metrics_enabled=True`.
- Defaults are unchanged unless you opt in to these features.
