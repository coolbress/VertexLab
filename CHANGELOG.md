# Changelog

## [0.3.0](https://github.com/coolbress/VertexLab/compare/vertex-lab-v0.2.2...vertex-lab-v0.3.0) (2026-03-27)


### Features

* **vertex-lab:** add CLI stub and align version metadata ([#285](https://github.com/coolbress/VertexLab/issues/285)) ([2c1ed40](https://github.com/coolbress/VertexLab/commit/2c1ed4023ebccb1705056288084be70075f82a4e))

## [0.2.2](https://github.com/coolbress/VertexLab/compare/vertex-lab-v0.2.1...vertex-lab-v0.2.2) (2026-03-27)


### Bug Fixes

* **vertex-lab:** restore PR automation label permissions ([#282](https://github.com/coolbress/VertexLab/issues/282)) ([52dfad3](https://github.com/coolbress/VertexLab/commit/52dfad39a8c9f85733e8e4296975e31dfaed0094))

## [0.2.1](https://github.com/coolbress/VertexLab/compare/vertex-lab-v0.2.0...vertex-lab-v0.2.1) (2026-03-27)


### Bug Fixes

* **vertex-lab:** restore issue forms by removing empty title fields ([#279](https://github.com/coolbress/VertexLab/issues/279)) ([c21f396](https://github.com/coolbress/VertexLab/commit/c21f396027602d9e20c5395ae14732962a69cc9c))

## [0.2.0](https://github.com/coolbress/VertexLab/compare/vertex-lab-v0.1.0...vertex-lab-v0.2.0) (2026-03-26)


### Features

* add benchmark baseline gating and configurable writer concurrency ([#197](https://github.com/coolbress/VertexLab/issues/197))   ([350bbbe](https://github.com/coolbress/VertexLab/commit/350bbbe3da3c9a997fcd6f43c2ea6010e04579e4))
* add RequestSpec.idempotent flag and honor it in retries ([#157](https://github.com/coolbress/VertexLab/issues/157)) ([6c73062](https://github.com/coolbress/VertexLab/commit/6c73062559c9b41bb4baff1b2f896532d4134b11))
* **api,docs:** expose package public API at root; add README ([#47](https://github.com/coolbress/VertexLab/issues/47)) ([aeec230](https://github.com/coolbress/VertexLab/commit/aeec230a4262254bd5c743ca14ecd4743b289985))
* Centralized constants, env-aware CLI, standardized logging, and request tracing ([10cd9bc](https://github.com/coolbress/VertexLab/commit/10cd9bcc803657df5b467833030d06ee74576e88))
* **cli/recover:** UX, error reporting, and docs improvements ([#60](https://github.com/coolbress/VertexLab/issues/60)) ([1cd77d1](https://github.com/coolbress/VertexLab/commit/1cd77d138386f7cbef06f80c158028cc115b1d95))
* complete workspace setup and configuration ([13c065d](https://github.com/coolbress/VertexLab/commit/13c065d980caaae189bb7dbf0f3bcd7891219230))
* **config,pipeline:** add dlq_enabled to disable DLQ spooling; structured logs and summaries; keep rescue path ([#98](https://github.com/coolbress/VertexLab/issues/98)) ([717d0c5](https://github.com/coolbress/VertexLab/commit/717d0c51356f382b3612d1d6fce66e02d2bdcfe0))
* **core/pipeline:** add DLQ spool and per‑packet rescue on writer flush failure ([#52](https://github.com/coolbress/VertexLab/issues/52))   ([bd26a65](https://github.com/coolbress/VertexLab/commit/bd26a6574c53af0310a8d0a84eb579a709b08999))
* **core/retry:** add server-side HTTP status retries (429/503) with configurable backoff ([#51](https://github.com/coolbress/VertexLab/issues/51)) ([17cac8b](https://github.com/coolbress/VertexLab/commit/17cac8be2c3343972e7e364db8ef60ebbcf6f15e))
* **core/retry:** switch to Full Jitter backoff; docs: opt‑in 500/502/504 while keeping 429/503 defaults ([#64](https://github.com/coolbress/VertexLab/issues/64)) ([0abc55d](https://github.com/coolbress/VertexLab/commit/0abc55d4c30120ed1af003c80e52d79fef98afff))
* **core:** tighten static typing, strict schema validation, and memory dedup ([#184](https://github.com/coolbress/VertexLab/issues/184)) ([691c535](https://github.com/coolbress/VertexLab/commit/691c535bd2e44c02dce77992507f8685f63d83f9))
* DuckDB identifier escaping, unified exceptions, HTTP log redaction, Sharadar validation; add tests and docs ([77abcef](https://github.com/coolbress/VertexLab/commit/77abceff935bd85def4e29951ef4bccb524a8230))
* **flow:** adaptive RPM downshift and recovery ([#97](https://github.com/coolbress/VertexLab/issues/97)) ([b780ef8](https://github.com/coolbress/VertexLab/commit/b780ef8f57abe53ff492689c242ac37b2e5811af))
* Implement resumable runs and run history persistence ([#193](https://github.com/coolbress/VertexLab/issues/193))  ([fcd1ab4](https://github.com/coolbress/VertexLab/commit/fcd1ab44f64f4a3bc6c7c4c00c084b3ea2a7de04))
* Implement structured errors and data quality validation ([#194](https://github.com/coolbress/VertexLab/issues/194))  ([b20778b](https://github.com/coolbress/VertexLab/commit/b20778bc92835d2686fbcbf65ee5d5f8696f1831))
* improve type precision, benchmark metric robustness, and yfinance parsing coverage ([#198](https://github.com/coolbress/VertexLab/issues/198)) ([c50d861](https://github.com/coolbress/VertexLab/commit/c50d861dbb378a4f709995041c277aab4b11e2e1))
* Initial commit with uv monorepo structure ([3d2beba](https://github.com/coolbress/VertexLab/commit/3d2bebad2cd37ddabb7c1995a6718777eca10d5c))
* **observability:** queue/DLQ/per-table metrics and optional spans; README metrics section ([#90](https://github.com/coolbress/VertexLab/issues/90)) ([6b62b0d](https://github.com/coolbress/VertexLab/commit/6b62b0d54c7d436bcf0ecc471fc3dd00e537f183))
* opt-in strict schema validation and in-memory dedup/upsert ([#153](https://github.com/coolbress/VertexLab/issues/153)) ([53840ac](https://github.com/coolbress/VertexLab/commit/53840ac2aef93d1e463f011320dd8c8c844c0d09))
* pagination fairness cap and unified shutdown ([#161](https://github.com/coolbress/VertexLab/issues/161)) ([a1fd63b](https://github.com/coolbress/VertexLab/commit/a1fd63b6bb30e65de8d81c326c6731dc20d6afcf))
* provider architecture standardization + yfinance integration ([dc51c1f](https://github.com/coolbress/VertexLab/commit/dc51c1ffd0aefa0a2c20713ae30f8ae04e5d257c))
* structured logs + lightweight metrics; CI-safe toggles ([deb29f6](https://github.com/coolbress/VertexLab/commit/deb29f6d175df0adadcabfc2aefd0222a9dd01de))
* **writers:** map pl.List and pl.Struct to native DuckDB nested types instead of VARCHAR ([#182](https://github.com/coolbress/VertexLab/issues/182))  ([46ddafe](https://github.com/coolbress/VertexLab/commit/46ddafeb1aa50689e2b2a3a912f9ae12064b97c6))
* **writer:** streamed chunked flush via writer_chunk_rows; preserve order/totals ([#91](https://github.com/coolbress/VertexLab/issues/91)) ([2375fcd](https://github.com/coolbress/VertexLab/commit/2375fcdc58414ebe75b45aa87538f9c5d7fe4c5f))


### Bug Fixes

* **controller:** measure RTT after slot acquisition to exclude queue wait ([#181](https://github.com/coolbress/VertexLab/issues/181))  ([b219240](https://github.com/coolbress/VertexLab/commit/b2192407806eb6aa888d71959595dc9a8195a2f7))
* make vertex_forager.logging a proper package; add import test  ([dea921f](https://github.com/coolbress/VertexLab/commit/dea921f1323cd681342a95c15d28797403ae2241))
* monorepo outputs usage in publish chain; add attach-assets workflow; pin release-please SHA ([#107](https://github.com/coolbress/VertexLab/issues/107)) ([16d1dc9](https://github.com/coolbress/VertexLab/commit/16d1dc9882df41d4f55f5a909bd8ea28343022cc))
* **pipeline:** deduplicate flush error reporting and keep DLQ ops intact ([#73](https://github.com/coolbress/VertexLab/issues/73)) ([2253025](https://github.com/coolbress/VertexLab/commit/2253025e70db7840d75d61c5e529c7d788153cab))
* respect show_progress in SharadarClient  ([#175](https://github.com/coolbress/VertexLab/issues/175)) ([c838aef](https://github.com/coolbress/VertexLab/commit/c838aef9641b95e05a3807ba608927fba57c72ab))
* serialize RunError in benchmark profile output ([#194](https://github.com/coolbress/VertexLab/issues/194))  ([dd91e9c](https://github.com/coolbress/VertexLab/commit/dd91e9c79784ca18c38ee6a454dafba0babbb716))
* **verification:** guard yfinance perf script for optional deps ([#207](https://github.com/coolbress/VertexLab/issues/207)) ([bc38dfd](https://github.com/coolbress/VertexLab/commit/bc38dfdb27c110232e3796f85bf8f5846a508074))
* **verification:** prevent optional yfinance deps from breaking performance checks ([#209](https://github.com/coolbress/VertexLab/issues/209)) ([8742799](https://github.com/coolbress/VertexLab/commit/87427999aa0c863aa8ab5cf51c90983b7fd9542e))


### Performance Improvements

* Add Tuning CLI and Optimize Hotspots ([c298f26](https://github.com/coolbress/VertexLab/commit/c298f26459b6c23480c9121c7ad9ab203f98462d))
* **writer/duckdb:** run CHECKPOINT after VACUUM to control WAL growth ([#65](https://github.com/coolbress/VertexLab/issues/65)) ([0174492](https://github.com/coolbress/VertexLab/commit/0174492e04f70f125b2b2a1c24ef1b2f9060daac))


### Dependencies

* update uv-build requirement in /packages/vertex-forager ([#123](https://github.com/coolbress/VertexLab/issues/123)) ([b5fe568](https://github.com/coolbress/VertexLab/commit/b5fe568c0d93c59a781a31052ffbcd89a197f4b2))
* update uv-build requirement in /packages/vertex-forager ([#213](https://github.com/coolbress/VertexLab/issues/213)) ([5bbfb8e](https://github.com/coolbress/VertexLab/commit/5bbfb8e9db45ad69e531efdcf3bcb87403b72fcc))


### Documentation

* add Architecture Decision Records and ADR navigation ([#229](https://github.com/coolbress/VertexLab/issues/229))  ([b79ed61](https://github.com/coolbress/VertexLab/commit/b79ed619dbe145f39f64445b35457b7d53cbbc12))
* add lint quick‑fix guide, per‑file‑ignores policy, and labels policy cross‑refs ([#150](https://github.com/coolbress/VertexLab/issues/150)) ([f5e9964](https://github.com/coolbress/VertexLab/commit/f5e9964b8a6c4bf6c66c7e408c696bc0a911349e))
* add Pre-0.2.0 historical section and Unreleased comparison link ([85d6746](https://github.com/coolbress/VertexLab/commit/85d6746fdab53a2714b90bf0f8a5b13c1681d361))
* claude guardrails & analysis ignore rules; absolute paths; security gates ([#179](https://github.com/coolbress/VertexLab/issues/179)) ([b759f0d](https://github.com/coolbress/VertexLab/commit/b759f0d741b8d29f709f4f9c599369075aba65ee))
* consolidate quality-gate checklist references ([#237](https://github.com/coolbress/VertexLab/issues/237)) ([a54c1cf](https://github.com/coolbress/VertexLab/commit/a54c1cfe21f4731f9c9d39a7d578a35ab4b73cce))
* **core/pipeline:** clarify unreachable FetchError due to tenacity reraise=True ([#69](https://github.com/coolbress/VertexLab/issues/69)) ([0058024](https://github.com/coolbress/VertexLab/commit/0058024c8b7bfbb63c2fd764c0318965c72795b6))
* expand API reference, add migration/testing guides and example notebooks ([#172](https://github.com/coolbress/VertexLab/issues/172)) ([187bd99](https://github.com/coolbress/VertexLab/commit/187bd991cdac788e6d532f38205e62fd057a88a1))
* modernize documentation, README, examples, and docs workflows (Diátaxis + mkdocstrings) ([#103](https://github.com/coolbress/VertexLab/issues/103)) ([ed05902](https://github.com/coolbress/VertexLab/commit/ed0590210a765c4efde8811035a7e34c7c060140))
* monorepo docs; API reference expansion; Node runtime policy; utils typecheck fix ([#125](https://github.com/coolbress/VertexLab/issues/125)) ([3d6daef](https://github.com/coolbress/VertexLab/commit/3d6daef75cf8068287772f12f918a21d6d4c2ab5))
* Standardize Google-style docstrings across public APIs; add examples; clarify behaviors ([fd14543](https://github.com/coolbress/VertexLab/commit/fd14543791c67adffacd18e004ff45ed7769ac60))
* **writer:** add fan‑out roadmap and central docs index ([#71](https://github.com/coolbress/VertexLab/issues/71)) ([876c94c](https://github.com/coolbress/VertexLab/commit/876c94cc9c7a244afca50ec9fd332480f1924fb5))
