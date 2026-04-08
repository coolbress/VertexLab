# CLI Reference

This page lists the stable public CLI surface.

## Top-level commands

- `vertex-forager collect`
- `vertex-forager status`
- `vertex-forager dlq`
- `vertex-forager runs`
- `vertex-forager checkpoints`
- `vertex-forager clear`
- `vertex-forager constants`
- `vertex-forager tune`

`constants` and `tune` remain available, but they are internal-facing commands rather than the main user workflow surface.

## `collect`

### Common flags

All dataset subcommands support:

- `--connect-db str`
- `--quality-check [warn|error]`
- `--no-progress`

### `collect sharadar price`

- `--symbol / -s` required, repeatable
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`

### `collect sharadar fundamentals`

- `--symbol / -s` required, repeatable
- `--dimension [MRY|MRQ|MRT|ARY|ARQ|ART]`, default `MRT`
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`

### `collect sharadar tickers`

- common flags only

### `collect sharadar sp500`

- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`

### `collect yfinance price`

- `--symbol / -s` required, repeatable
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`

### `collect yfinance financials`

- `--symbol / -s` required, repeatable
- `--kind [income_stmt|balance_sheet|cashflow]` required
- `--period [annual|quarterly]`, default `annual`

### `collect yfinance info`

- `--symbol / -s` required, repeatable

### `collect yfinance dividends`

- `--symbol / -s` required, repeatable
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`

## `status`

```bash
vertex-forager status
```

Shows:

- storage root path
- cache dir
- state DB path
- total data size
- checkpoint counts per table
- pending DLQ counts per table
- last run timestamp per table

## `dlq`

### `dlq list`

- `--table str`
- `--status [pending|recovered|all]`, default `pending`

### `dlq replay`

- `--table str` required
- `--output str`
- `--dry-run`

### `dlq clear`

- `--table str`
- `--all`

Provide exactly one of `--table` or `--all`.

## `runs`

### `runs list`

- `--table str`
- `--limit int`, default `20`

### `runs clear`

- `--table str`
- `--before <Nd>`

Provide at least one of `--table` or `--before`.

## `checkpoints`

### `checkpoints resume`

- `--table str` required
- `--output str` required

### `checkpoints clear`

- `--table str`
- `--all`

Provide exactly one of `--table` or `--all`.

## `clear`

- `--checkpoints`
- `--runs`
- `--dlq`
- `--all`

Destructive operations prompt for confirmation interactively.

## Related pages

- [Collect data](../how-to/collect-data.md)
- [Manage local state](../how-to/manage-local-state.md)
