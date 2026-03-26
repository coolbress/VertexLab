# Claude Prompt Snippets

## Architecture Review (≤5 Top Risks)

```text
Goal:
- Identify top risks across correctness, integrity, security, performance, reliability, and quality.
Output:
- Risk summary with proposals
- Minimal file links
- Next actions (commands/edits)
```

## Bug Fix (Minimal Change)

```text
Goal:
- Repro → suspect cause (1–2) → minimal fix
Verify:
- ruff/mypy/pytest/cycle checks pass
Output:
- Change summary → file links → commands
```

## Performance Tuning (Top-2 Bottlenecks)

```text
Goal:
- Pinpoint bottlenecks → safe tuning plan (concurrency/Polars/httpx)
Verify:
- Compare profiling metrics (p95/rows/memory)
Output:
- Summary → links → commands
```
