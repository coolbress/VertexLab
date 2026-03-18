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

### Internal

- Strict docs build (`mkdocs build --strict`) and Markdown/HTML link checker (lychee)
- GitHub Pages deploy workflow for docs site
- CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md
- Issue Forms (bug report, feature request, docs improvement) and PR template aligned with guard/labeler workflows
- `pyproject.toml` metadata: `project.urls`, `license`, `classifiers`; `docs` extras

## [Pre-0.2.0] - Historical

### Added

- Switch to Full Jitter backoff; docs: opt‑in 500/502/504 while keeping 429/503 defaults  (#64)
- UX, error reporting, and docs improvements  (#60)
- Add DLQ spool and per-packet rescue on writer flush failure (#59)
- Add server-side HTTP status retries (429/503) with configurable backoff  (#51)
- Expose package public API at root; add README  (#47)
- Structured logs and lightweight metrics with CI-safe toggles (#27)
- Centralize constants; standardize logging; English-only docs/CLI; constants preview with env overrides; trace/request correlation (#21)
- DuckDB identifier escaping, exceptions hierarchy, secrets masking, structured logging; add tests and docs (#18)
- Provider architecture standardization + yfinance integration (#6)

### Changed

- Run CHECKPOINT after VACUUM to control WAL growth  (#65)
- Replace pickle serialization in library fetch with Arrow/Polars IPC; JSON fallback  (#49)
- Validate EngineConfig at construction; remove DuckDBWriter globals; unify pipeline orchestration  (#48)
- Dead code cleanup, exception consistency, concurrency validation  (#42)
- Strict mypy baseline for vertex-forager; minimal fixes  (#38)
- Add Tuning CLI and Optimize Hotspots (#29)
- Apply DIP, add library fetcher registry, extend yfinance, strengthen CI/docs (#23)
- Refactor: Strengthen type safety and introduce generics across routers/clients (#20)
- Migrate Vertex Forager to Monorepo & Establish Extensible Architecture (#2)

### Fixed

- Deduplicate flush error reporting; keep DLQ ops intact (#73)
- Make vertex_forager.logging a proper package; add import test (#35)

### Docs

- Add writer fan‑out roadmap and central docs index (#71)
- Clarify unreachable FetchError due to tenacity reraise=true (#69)
- Standardize Google-style docstrings across public APIs; add examples; clarify behaviors (#22)
- Refactor: Decompose Router/Client into cohesive submodules, apply DIP, standardize jobs/errors (#19)

### Internal

- Standardize issue/PR templates and docs gates (#101)
- Harden limiters; add GCRA/gradient and PK/DLQ checks (#99)
- Bump astral‑sh/setup‑uv action to 7.5.0 (#96)
- Bump actionlint to 0.1.11 (#95)
- Add actionlint; concurrency/timeout; labeler fallback; dependabot policy (#94)
- Bump tornado to 6.5.5 (#93)
- Force Node 24; reliable type labels; human‑only auto‑review (#92)
- Enforce mypy --strict and CI; fix typing in routers/fetcher (#89)
- PR body guard; type labels from title (#80)
- Cache uv/trivy; build sdist/wheel; add pip‑audit (#79)
- Improve test coverage and operational maturity roadmap (#70)
- Bump astral-sh/setup-uv to 7.4.0 (#68)
- Provider plugin guide; validate transport decoupling; unit/integration tests  (#40)
- Expand coverage to ≥80%, stabilize suite; mark integrations (#25)

## [0.1.0] - 2026-03-15

### Added

- Initial release of Vertex Forager engine:
  - Core pipeline orchestration (`VertexForager`), configuration (`EngineConfig`), rate limiting (`FlowController`), HTTP executor, and retry strategies
  - Providers: YFinance and Sharadar clients/routers
  - Writers: DuckDB writer with transactional upsert; in‑memory writer
  - Schema registry and mapper; standardized datasets and types
  - CLI commands: `status`, `constants`, `collect` (Sharadar)
- Type hints and strict typing; unit/integration tests for core components

[Unreleased]: https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.2.0...HEAD
