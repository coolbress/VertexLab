# Changelog

## [1.0.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.7...vertex-forager-v1.0.0) (2026-03-31)


### ⚠ BREAKING CHANGES

* **vertex-forager:** move checkpoints and run history into SQLite state.db with DLQ index, retention  cleanup, and CLI state commands ([#383](https://github.com/coolbress/VertexLab/issues/383))

### Features

* **vertex-forager:** move checkpoints and run history into SQLite state.db with DLQ index, retention  cleanup, and CLI state commands ([#383](https://github.com/coolbress/VertexLab/issues/383)) ([8739cfc](https://github.com/coolbress/VertexLab/commit/8739cfc9eea8497d73bd9a9de1818ea4eb8a998c))

## [0.11.7](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.6...vertex-forager-v0.11.7) (2026-03-28)


### Documentation

* **vertex-forager:** rewrite landing page and quickstart for users ([#332](https://github.com/coolbress/VertexLab/issues/332)) ([0f6a2eb](https://github.com/coolbress/VertexLab/commit/0f6a2eb587981285346dc7ad586445933c5c5ad6))

## [0.11.6](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.5...vertex-forager-v0.11.6) (2026-03-28)


### Documentation

* **vertex-forager:** reclassify provider docs around built-in architecture ([#329](https://github.com/coolbress/VertexLab/issues/329)) ([64729f4](https://github.com/coolbress/VertexLab/commit/64729f40064694b22440fc08982a027c877f8496))

## [0.11.5](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.4...vertex-forager-v0.11.5) (2026-03-28)


### Documentation

* **vertex-forager:** align docs with Diátaxis-style user flows ([#312](https://github.com/coolbress/VertexLab/issues/312)) ([559c618](https://github.com/coolbress/VertexLab/commit/559c61869e5846f7ecaab7c520450f37570896d4))

## [0.11.4](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.3...vertex-forager-v0.11.4) (2026-03-28)


### Documentation

* **vertex-forager:** improve landing page and align docs nav ([#301](https://github.com/coolbress/VertexLab/issues/301)) ([5617664](https://github.com/coolbress/VertexLab/commit/56176644f40e4076b497f83bf8900265b63deb65))

## [0.11.3](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.2...vertex-forager-v0.11.3) (2026-03-27)


### Documentation

* **vertex-lab:** rewrite contributing guide, relocate to docs/, and consolidate version-file ([#296](https://github.com/coolbress/VertexLab/issues/296)) ([d8430e7](https://github.com/coolbress/VertexLab/commit/d8430e7a0df0e759dec1aba41dc64c6f3db2791c))

## [0.11.2](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.1...vertex-forager-v0.11.2) (2026-03-26)


### Documentation

* add Architecture Decision Records and ADR navigation ([#229](https://github.com/coolbress/VertexLab/issues/229))  ([b79ed61](https://github.com/coolbress/VertexLab/commit/b79ed619dbe145f39f64445b35457b7d53cbbc12))

## [0.11.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.0...vertex-forager-v0.11.1) (2026-03-26)


### Documentation

* consolidate quality-gate checklist references ([#237](https://github.com/coolbress/VertexLab/issues/237)) ([a54c1cf](https://github.com/coolbress/VertexLab/commit/a54c1cfe21f4731f9c9d39a7d578a35ab4b73cce))

## [0.11.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.10.0...vertex-forager-v0.11.0) (2026-03-25)


### Features

* improve type precision, benchmark metric robustness, and yfinance parsing coverage ([#198](https://github.com/coolbress/VertexLab/issues/198)) ([c50d861](https://github.com/coolbress/VertexLab/commit/c50d861dbb378a4f709995041c277aab4b11e2e1))

## [0.10.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.9.4...vertex-forager-v0.10.0) (2026-03-24)


### Features

* add benchmark baseline gating and configurable writer concurrency ([#197](https://github.com/coolbress/VertexLab/issues/197))   ([350bbbe](https://github.com/coolbress/VertexLab/commit/350bbbe3da3c9a997fcd6f43c2ea6010e04579e4))

## [0.9.4](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.9.3...vertex-forager-v0.9.4) (2026-03-24)


### Dependencies

* update uv-build requirement in /packages/vertex-forager ([#213](https://github.com/coolbress/VertexLab/issues/213)) ([5bbfb8e](https://github.com/coolbress/VertexLab/commit/5bbfb8e9db45ad69e531efdcf3bcb87403b72fcc))

## [0.9.3](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.9.2...vertex-forager-v0.9.3) (2026-03-24)


### Bug Fixes

* **verification:** prevent optional yfinance deps from breaking performance checks ([#209](https://github.com/coolbress/VertexLab/issues/209)) ([8742799](https://github.com/coolbress/VertexLab/commit/87427999aa0c863aa8ab5cf51c90983b7fd9542e))

## [0.9.2](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.9.1...vertex-forager-v0.9.2) (2026-03-24)


### Bug Fixes

* **verification:** guard yfinance perf script for optional deps ([#207](https://github.com/coolbress/VertexLab/issues/207)) ([bc38dfd](https://github.com/coolbress/VertexLab/commit/bc38dfdb27c110232e3796f85bf8f5846a508074))

## [0.9.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.9.0...vertex-forager-v0.9.1) (2026-03-24)


### Bug Fixes

* serialize RunError in benchmark profile output ([#194](https://github.com/coolbress/VertexLab/issues/194))  ([dd91e9c](https://github.com/coolbress/VertexLab/commit/dd91e9c79784ca18c38ee6a454dafba0babbb716))

### Breaking Changes

* move `nest-asyncio`, `pandas`, and `yfinance` out of default installation; use extras for optional environments/features
* install optional dependencies explicitly with `pip install vertex-forager[notebook]` and `pip install vertex-forager[yfinance]`

## [0.9.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.8.0...vertex-forager-v0.9.0) (2026-03-24)


### Features

* Implement structured errors and data quality validation ([#194](https://github.com/coolbress/VertexLab/issues/194))  ([b20778b](https://github.com/coolbress/VertexLab/commit/b20778bc92835d2686fbcbf65ee5d5f8696f1831))

## [0.8.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.7.0...vertex-forager-v0.8.0) (2026-03-23)


### Features

* Implement resumable runs and run history persistence ([#193](https://github.com/coolbress/VertexLab/issues/193))  ([fcd1ab4](https://github.com/coolbress/VertexLab/commit/fcd1ab44f64f4a3bc6c7c4c00c084b3ea2a7de04))

## [0.7.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.6.0...vertex-forager-v0.7.0) (2026-03-23)


### Features

* **core:** tighten static typing, strict schema validation, and memory dedup ([#184](https://github.com/coolbress/VertexLab/issues/184)) ([691c535](https://github.com/coolbress/VertexLab/commit/691c535bd2e44c02dce77992507f8685f63d83f9))

## [0.6.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.5.3...vertex-forager-v0.6.0) (2026-03-23)


### Features

* **writers:** map pl.List and pl.Struct to native DuckDB nested types instead of VARCHAR ([#182](https://github.com/coolbress/VertexLab/issues/182))  ([46ddafe](https://github.com/coolbress/VertexLab/commit/46ddafeb1aa50689e2b2a3a912f9ae12064b97c6))

## [0.5.3](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.5.2...vertex-forager-v0.5.3) (2026-03-23)


### Bug Fixes

* **controller:** measure RTT after slot acquisition to exclude queue wait ([#181](https://github.com/coolbress/VertexLab/issues/181))  ([b219240](https://github.com/coolbress/VertexLab/commit/b2192407806eb6aa888d71959595dc9a8195a2f7))

## [0.5.2](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.5.1...vertex-forager-v0.5.2) (2026-03-21)


### Bug Fixes

* respect show_progress in SharadarClient  ([#175](https://github.com/coolbress/VertexLab/issues/175)) ([c838aef](https://github.com/coolbress/VertexLab/commit/c838aef9641b95e05a3807ba608927fba57c72ab))

## [0.5.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.5.0...vertex-forager-v0.5.1) (2026-03-21)


### Documentation

* expand API reference, add migration/testing guides and example notebooks ([#172](https://github.com/coolbress/VertexLab/issues/172)) ([187bd99](https://github.com/coolbress/VertexLab/commit/187bd991cdac788e6d532f38205e62fd057a88a1))

## [0.5.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.4.0...vertex-forager-v0.5.0) (2026-03-21)


### Features

* pagination fairness cap and unified shutdown ([#161](https://github.com/coolbress/VertexLab/issues/161)) ([a1fd63b](https://github.com/coolbress/VertexLab/commit/a1fd63b6bb30e65de8d81c326c6731dc20d6afcf))

## [0.4.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.3.0...vertex-forager-v0.4.0) (2026-03-20)


### Features

* add RequestSpec.idempotent flag and honor it in retries ([#157](https://github.com/coolbress/VertexLab/issues/157)) ([6c73062](https://github.com/coolbress/VertexLab/commit/6c73062559c9b41bb4baff1b2f896532d4134b11))

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
