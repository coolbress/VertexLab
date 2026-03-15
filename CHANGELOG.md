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

## [0.1.0] - 2026-03-15

### Added
- Initial release of Vertex Forager engine:
  - Core pipeline orchestration (`VertexForager`), configuration (`EngineConfig`), rate limiting (`FlowController`), HTTP executor, and retry strategies
  - Providers: YFinance and Sharadar clients/routers
  - Writers: DuckDB writer with transactional upsert; in‑memory writer
  - Schema registry and mapper; standardized datasets and types
  - CLI commands: `status`, `constants`, `collect` (Sharadar)
- Type hints and strict typing; unit/integration tests for core components
