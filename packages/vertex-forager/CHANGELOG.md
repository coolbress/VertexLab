# Changelog

## [0.3.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.2.2...vertex-forager-v0.3.0) (2026-03-20)


### Features

* opt-in strict schema validation and in-memory dedup/upsert ([#153](https://github.com/coolbress/VertexLab/issues/153)) ([53840ac](https://github.com/coolbress/VertexLab/commit/53840ac2aef93d1e463f011320dd8c8c844c0d09))

## [0.2.2](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.2.1...vertex-forager-v0.2.2) (2026-03-18)


### Documentation

* monorepo docs; API reference expansion; Node runtime policy; utils typecheck fix ([#125](https://github.com/coolbress/VertexLab/issues/125)) ([3d6daef](https://github.com/coolbress/VertexLab/commit/3d6daef75cf8068287772f12f918a21d6d4c2ab5))

## [0.2.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.2.0...vertex-forager-v0.2.1) (2026-03-18)


### Dependencies

* update uv-build requirement in /packages/vertex-forager ([#123](https://github.com/coolbress/VertexLab/issues/123)) ([b5fe568](https://github.com/coolbress/VertexLab/commit/b5fe568c0d93c59a781a31052ffbcd89a197f4b2))

## [0.2.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.1.0...vertex-forager-v0.2.0) (2026-03-16)


### Features

* **api,docs:** expose package public API at root; add README ([#47](https://github.com/coolbress/VertexLab/issues/47)) ([aeec230](https://github.com/coolbress/VertexLab/commit/aeec230a4262254bd5c743ca14ecd4743b289985))
* Centralized constants, env-aware CLI, standardized logging, and request tracing ([10cd9bc](https://github.com/coolbress/VertexLab/commit/10cd9bcc803657df5b467833030d06ee74576e88))
* **cli/recover:** UX, error reporting, and docs improvements ([#60](https://github.com/coolbress/VertexLab/issues/60)) ([1cd77d1](https://github.com/coolbress/VertexLab/commit/1cd77d138386f7cbef06f80c158028cc115b1d95))
* complete workspace setup and configuration ([13c065d](https://github.com/coolbress/VertexLab/commit/13c065d980caaae189bb7dbf0f3bcd7891219230))
* **config,pipeline:** add dlq_enabled to disable DLQ spooling; structured logs and summaries; keep rescue path ([#98](https://github.com/coolbress/VertexLab/issues/98)) ([717d0c5](https://github.com/coolbress/VertexLab/commit/717d0c51356f382b3612d1d6fce66e02d2bdcfe0))
* **core/pipeline:** add DLQ spool and per‑packet rescue on writer flush failure ([#52](https://github.com/coolbress/VertexLab/issues/52))   ([bd26a65](https://github.com/coolbress/VertexLab/commit/bd26a6574c53af0310a8d0a84eb579a709b08999))
* **core/retry:** add server-side HTTP status retries (429/503) with configurable backoff ([#51](https://github.com/coolbress/VertexLab/issues/51)) ([17cac8b](https://github.com/coolbress/VertexLab/commit/17cac8be2c3343972e7e364db8ef60ebbcf6f15e))
* **core/retry:** switch to Full Jitter backoff; docs: opt‑in 500/502/504 while keeping 429/503 defaults ([#64](https://github.com/coolbress/VertexLab/issues/64)) ([0abc55d](https://github.com/coolbress/VertexLab/commit/0abc55d4c30120ed1af003c80e52d79fef98afff))
* DuckDB identifier escaping, unified exceptions, HTTP log redaction, Sharadar validation; add tests and docs ([77abcef](https://github.com/coolbress/VertexLab/commit/77abceff935bd85def4e29951ef4bccb524a8230))
* **flow:** adaptive RPM downshift and recovery ([#97](https://github.com/coolbress/VertexLab/issues/97)) ([b780ef8](https://github.com/coolbress/VertexLab/commit/b780ef8f57abe53ff492689c242ac37b2e5811af))
* Initial commit with uv monorepo structure ([3d2beba](https://github.com/coolbress/VertexLab/commit/3d2bebad2cd37ddabb7c1995a6718777eca10d5c))
* **observability:** queue/DLQ/per-table metrics and optional spans; README metrics section ([#90](https://github.com/coolbress/VertexLab/issues/90)) ([6b62b0d](https://github.com/coolbress/VertexLab/commit/6b62b0d54c7d436bcf0ecc471fc3dd00e537f183))
* provider architecture standardization + yfinance integration ([dc51c1f](https://github.com/coolbress/VertexLab/commit/dc51c1ffd0aefa0a2c20713ae30f8ae04e5d257c))
* structured logs + lightweight metrics; CI-safe toggles ([deb29f6](https://github.com/coolbress/VertexLab/commit/deb29f6d175df0adadcabfc2aefd0222a9dd01de))
* **writer:** streamed chunked flush via writer_chunk_rows; preserve order/totals ([#91](https://github.com/coolbress/VertexLab/issues/91)) ([2375fcd](https://github.com/coolbress/VertexLab/commit/2375fcdc58414ebe75b45aa87538f9c5d7fe4c5f))


### Bug Fixes

* make vertex_forager.logging a proper package; add import test  ([dea921f](https://github.com/coolbress/VertexLab/commit/dea921f1323cd681342a95c15d28797403ae2241))
* **pipeline:** deduplicate flush error reporting and keep DLQ ops intact ([#73](https://github.com/coolbress/VertexLab/issues/73)) ([2253025](https://github.com/coolbress/VertexLab/commit/2253025e70db7840d75d61c5e529c7d788153cab))


### Performance Improvements

* Add Tuning CLI and Optimize Hotspots ([c298f26](https://github.com/coolbress/VertexLab/commit/c298f26459b6c23480c9121c7ad9ab203f98462d))
* **writer/duckdb:** run CHECKPOINT after VACUUM to control WAL growth ([#65](https://github.com/coolbress/VertexLab/issues/65)) ([0174492](https://github.com/coolbress/VertexLab/commit/0174492e04f70f125b2b2a1c24ef1b2f9060daac))


### Documentation

* **core/pipeline:** clarify unreachable FetchError due to tenacity reraise=True ([#69](https://github.com/coolbress/VertexLab/issues/69)) ([0058024](https://github.com/coolbress/VertexLab/commit/0058024c8b7bfbb63c2fd764c0318965c72795b6))
* modernize documentation, README, examples, and docs workflows (Diátaxis + mkdocstrings) ([#103](https://github.com/coolbress/VertexLab/issues/103)) ([ed05902](https://github.com/coolbress/VertexLab/commit/ed0590210a765c4efde8811035a7e34c7c060140))
* Standardize Google-style docstrings across public APIs; add examples; clarify behaviors ([fd14543](https://github.com/coolbress/VertexLab/commit/fd14543791c67adffacd18e004ff45ed7769ac60))
