# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Tutorials and how‑to pages (Diátaxis): Quickstart; DLQ disabled; chunked flush; performance tuning; troubleshooting; provider extension/plugin; CLI equivalents
- Examples: uv‑runnable scripts for YFinance (in‑memory, DuckDB metrics) and Sharadar (in‑memory)
- Explanation pages: Router ↔ Client diagrams (Mermaid); Writer upsert behavior

### Changed
- README overhaul with badges (Python versions, License, CI) and embedded architecture diagram
- mkdocs.yml navigation with Material theme and mkdocstrings for API reference

### CI
- Strict docs build (`mkdocs build --strict`) and Markdown/HTML link checker (lychee)
- GitHub Pages deploy workflow for docs site

### Community & Packaging
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md
- Issue Forms (bug report, feature request, docs improvement) and PR template aligned with guard/labeler workflows
- `pyproject.toml` metadata: `project.urls`, `license`, `classifiers`; `docs` extras

## [Pre-0.2.0] - Historical

### Added
- feat: switch to Full Jitter backoff; docs: opt‑in 500/502/504 while keeping 429/503 defaults  (#64)
- feat: UX, error reporting, and docs improvements  (#60)
- feat: add DLQ spool and per-packet rescue on writer flush failure (#59)
- feat: add server-side HTTP status retries (429/503) with configurable backoff  (#51)
- feat: expose package public API at root; add README  (#47)
- feat: structured logs and lightweight metrics with CI-safe toggles (#27)
- feat: Centralize constants; standardize logging; English-only docs/CLI; constants preview with env overrides; trace/request correlation (#21)
- feat: DuckDB identifier escaping, exceptions hierarchy, secrets masking, structured logging; add tests and docs (#18)
- feat: provider architecture standardization + yfinance integration (#6)

### Changed
- perf: run CHECKPOINT after VACUUM to control WAL growth  (#65)
- refactor: replace pickle serialization in library fetch with Arrow/Polars IPC; JSON fallback  (#49)
- refactor: validate EngineConfig at construction; remove DuckDBWriter globals; unify pipeline orchestration  (#48)
- refactor: dead code cleanup, exception consistency, concurrency validation  (#42)
- refactor: types(mypy): strict baseline for vertex-forager; minimal fixes  (#38)
- perf: Add Tuning CLI and Optimize Hotspots (#29)
- refactor: Apply DIP, add library fetcher registry, extend yfinance, strengthen CI/docs (#23)
- refactor: Refactor: Strengthen type safety and introduce generics across routers/clients (#20)
- refactor: Migrate Vertex Forager to Monorepo & Establish Extensible Architecture (#2)

### Fixed
- fix: deduplicate flush error reporting; keep DLQ ops intact (#73)
- fix: make vertex_forager.logging a proper package; add import test (#35)

### Docs
- docs: add writer fan‑out roadmap and central docs index (#71)
- docs: clarify unreachable FetchError due to tenacity reraise=true (#69)
- docs: Standardize Google-style docstrings across public APIs; add examples; clarify behaviors (#22)
- docs: # Refactor: Decompose Router/Client into cohesive submodules, apply DIP, standardize jobs/errors (#19)

### Internal
- chore: standardize issue/PR templates and docs gates (#101)
- test: harden limiters; add GCRA/gradient and PK/DLQ checks (#99)
- chore: bump setup‑uv action to 7.5.0 (#96)
- chore: bump actionlint to 0.1.11 (#95)
- chore: add actionlint; concurrency/timeout; labeler fallback; dependabot policy (#94)
- chore: bump tornado to 6.5.5 (#93)
- chore: force Node 24; reliable type labels; human‑only auto‑review (#92)
- chore: enforce mypy --strict and CI; fix typing in routers/fetcher (#89)
- chore: PR body guard; type labels from title (#80)
- chore: cache uv/trivy; build sdist/wheel; add pip‑audit (#79)
- chore: improve test coverage and operational maturity roadmap (#70)
- chore: bump astral-sh/setup-uv to 7.4.0 (#68)
- chore: provider plugin guide; validate transport decoupling; unit/integration tests  (#40)
- test: expand coverage to ≥80%, stabilize suite; mark integrations (#25)

## [0.1.0] - 2026-03-15

### Added
- Initial release of Vertex Forager engine:
  - Core pipeline orchestration (`VertexForager`), configuration (`EngineConfig`), rate limiting (`FlowController`), HTTP executor, and retry strategies
  - Providers: YFinance and Sharadar clients/routers
  - Writers: DuckDB writer with transactional upsert; in‑memory writer
  - Schema registry and mapper; standardized datasets and types
  - CLI commands: `status`, `constants`, `collect` (Sharadar)
- Type hints and strict typing; unit/integration tests for core components
