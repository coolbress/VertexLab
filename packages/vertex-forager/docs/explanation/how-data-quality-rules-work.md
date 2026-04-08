# How Data Quality Rules Work

Data quality rules are evaluated on normalized table-shaped data just before persistence.

## When rules run

Rules run after normalization and before write completion.

That means the rules see the internal storage contract, not the provider’s raw payload shape.

## Why normalization comes first

Rules need stable column names and types to behave consistently.

By running after normalization, vertex-forager can evaluate the same rule set against:

- full provider payloads
- partial provider payloads
- frames that arrived through different fetch paths

without changing the rule logic each time.

## `warn` versus `error`

With `quality_check="warn"`:

- rule violations are recorded in `RunResult.quality_violations`
- the run can continue

With `quality_check="error"`:

- the first violating flush raises `DataQualityError`
- the run stops instead of continuing with known-bad data

## How violations accumulate

Violations accumulate by table in `RunResult.quality_violations`.

The result is operational rather than row-by-row forensic detail. It answers which table violated which workflow expectation and how often, so you can decide whether to re-fetch, inspect source data, or change quality mode.

## Built-in rules

The built-in public rules include:

- `NoDuplicateRows`
- `NoFutureDates`
- `NoNegativePrices`

Each rule is attached through `TableSchema`, so the rule set follows the table definition rather than ad hoc call-site logic.

These rule classes are part of the internal schema contract. They are not a primary end-user configuration surface exposed from the package root.

## Why this matters

Quality checks are not a second storage schema. They are the final guardrail between normalized data and the target writer.

## Related pages

- [Interpret run results](../how-to/interpret-run-results.md)
- [How schema normalization works](how-schema-normalization-works.md)
- [Schema reference](../reference/schema.md)
