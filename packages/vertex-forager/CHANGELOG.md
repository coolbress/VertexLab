# Changelog

## [0.30.4](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.30.3...vertex-forager-v0.30.4) (2026-04-09)


### Bug Fixes

* **vertex-forager:** add breaking change guards ([#491](https://github.com/coolbress/VertexLab/issues/491)) ([34bf4df](https://github.com/coolbress/VertexLab/commit/34bf4dfd089263f15a96372cfef7991aa3a7585f))

## [0.30.3](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.30.2...vertex-forager-v0.30.3) (2026-04-09)


### Bug Fixes

* **vertex-forager:** harden writer correctness paths ([#489](https://github.com/coolbress/VertexLab/issues/489)) ([cf6d01b](https://github.com/coolbress/VertexLab/commit/cf6d01b6d2938765e33e1a9743675ada978f04ba))

## [0.30.2](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.30.1...vertex-forager-v0.30.2) (2026-04-09)


### Bug Fixes

* **vertex-forager:** tolerate polars errors in non-strict schema mapping ([#485](https://github.com/coolbress/VertexLab/issues/485)) ([d3d5a37](https://github.com/coolbress/VertexLab/commit/d3d5a37a2cc03bb0ea8061a64ee6e982e4f6daf6))
* **vertex-forager:** tolerate polars errors in non-strict schema mapping ([#488](https://github.com/coolbress/VertexLab/issues/488)) ([c5c737e](https://github.com/coolbress/VertexLab/commit/c5c737ea0848ecd8e26293e15b0db2e33c655dd6))

## [0.30.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.30.0...vertex-forager-v0.30.1) (2026-04-09)


### Bug Fixes

* **vertex-forager:** serialize flow controller feedback ([#483](https://github.com/coolbress/VertexLab/issues/483)) ([44e514a](https://github.com/coolbress/VertexLab/commit/44e514a27abfe2726297807acf15a88f613e8482))

## [0.30.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.29.0...vertex-forager-v0.30.0) (2026-04-08)


### ⚠ BREAKING CHANGES

* **vertex-forager:** align docs and SDK public surface ([#470](https://github.com/coolbress/VertexLab/issues/470))

### Features

* **vertex-forager:** align docs and SDK public surface ([#470](https://github.com/coolbress/VertexLab/issues/470)) ([d82a70a](https://github.com/coolbress/VertexLab/commit/d82a70a567e3ab9917985a46864d0b040a521e3c))

## [0.29.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.28.0...vertex-forager-v0.29.0) (2026-04-08)


### ⚠ BREAKING CHANGES

* **vertex-forager:** align CLI with stable SDK surface ([#468](https://github.com/coolbress/VertexLab/issues/468))

### Features

* **vertex-forager:** align CLI with stable SDK surface ([#468](https://github.com/coolbress/VertexLab/issues/468)) ([27b949c](https://github.com/coolbress/VertexLab/commit/27b949c513c21725b54d89cfb01eeb69746f0bab))

## [0.28.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.27.0...vertex-forager-v0.28.0) (2026-04-07)


### ⚠ BREAKING CHANGES

* **vertex-forager:** add table-based state manager APIs ([#466](https://github.com/coolbress/VertexLab/issues/466))

### Features

* **vertex-forager:** add table-based state manager APIs ([#466](https://github.com/coolbress/VertexLab/issues/466)) ([4ee8222](https://github.com/coolbress/VertexLab/commit/4ee8222124d6138f49d6064784132ca13d23ba7d))

## [0.27.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.26.0...vertex-forager-v0.27.0) (2026-04-07)


### ⚠ BREAKING CHANGES

* **vertex-forager:** align persisted schema contracts  ([#452](https://github.com/coolbress/VertexLab/issues/452))

### Bug Fixes

* **vertex-forager:** align persisted schema contracts  ([#452](https://github.com/coolbress/VertexLab/issues/452)) ([168a095](https://github.com/coolbress/VertexLab/commit/168a095c090701533d19335db0a13efb183f3038))

## [0.26.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.25.0...vertex-forager-v0.26.0) (2026-04-04)


### ⚠ BREAKING CHANGES

* **vertex-forager:** add provider-typed create_client overloads ([#439](https://github.com/coolbress/VertexLab/issues/439))

### Features

* **vertex-forager:** add provider-typed create_client overloads ([#439](https://github.com/coolbress/VertexLab/issues/439)) ([6169e94](https://github.com/coolbress/VertexLab/commit/6169e945005cd0b0b5dce051504986d15f5842cf))

## [0.25.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.24.0...vertex-forager-v0.25.0) (2026-04-04)


### ⚠ BREAKING CHANGES

* **vertex-forager:** wire data quality into the SDK and unify RunResult ([#437](https://github.com/coolbress/VertexLab/issues/437))

### Features

* **vertex-forager:** wire data quality into the SDK and unify RunResult ([#437](https://github.com/coolbress/VertexLab/issues/437)) ([d038530](https://github.com/coolbress/VertexLab/commit/d038530bee126b5f7f194eec07ac3a1402fd9761))

## [0.24.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.23.0...vertex-forager-v0.24.0) (2026-04-03)


### ⚠ BREAKING CHANGES

* **vertex-forager:** introduce StorageConfig, move http_timeout_s into HTTPConfig, and cleanup config layer ([#434](https://github.com/coolbress/VertexLab/issues/434))

### Code Refactoring

* **vertex-forager:** introduce StorageConfig, move http_timeout_s into HTTPConfig, and cleanup config layer ([#434](https://github.com/coolbress/VertexLab/issues/434)) ([ba65122](https://github.com/coolbress/VertexLab/commit/ba65122d8906906eac90f19b71455ba167721110))

## [0.23.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.22.0...vertex-forager-v0.23.0) (2026-04-03)


### ⚠ BREAKING CHANGES

* **vertex-forager:** align vertex-forager tracing with OpenTelemetry standard and remove AdvancedConfig ([#431](https://github.com/coolbress/VertexLab/issues/431))

### Features

* **vertex-forager:** align vertex-forager tracing with OpenTelemetry standard and remove AdvancedConfig ([#431](https://github.com/coolbress/VertexLab/issues/431)) ([2690255](https://github.com/coolbress/VertexLab/commit/2690255a961f7dc7f38a40081b027ac02939c591))

## [0.22.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.21.0...vertex-forager-v0.22.0) (2026-04-02)


### ⚠ BREAKING CHANGES

* **vertex-forager:** align logging model with OSS Python standard ([#429](https://github.com/coolbress/VertexLab/issues/429))

### Code Refactoring

* **vertex-forager:** align logging model with OSS Python standard ([#429](https://github.com/coolbress/VertexLab/issues/429)) ([407eba4](https://github.com/coolbress/VertexLab/commit/407eba4022548e39240487cb58a18fe7a4b1fa19))

## [0.21.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.20.0...vertex-forager-v0.21.0) (2026-04-02)


### ⚠ BREAKING CHANGES

* **vertex-forager:** remove 6 deprecated client parameters and dead code ([#426](https://github.com/coolbress/VertexLab/issues/426))

### Code Refactoring

* **vertex-forager:** remove 6 deprecated client parameters and dead code ([#426](https://github.com/coolbress/VertexLab/issues/426)) ([c9eae53](https://github.com/coolbress/VertexLab/commit/c9eae5387de288bfaa5f7e5cc98dd3a6ac38db1e))

## [0.20.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.19.1...vertex-forager-v0.20.0) (2026-04-02)


### ⚠ BREAKING CHANGES

* **vertex-forager:** replace implicit Sharadar metadata cache with explicit meta input ([#423](https://github.com/coolbress/VertexLab/issues/423))

### Code Refactoring

* **vertex-forager:** replace implicit Sharadar metadata cache with explicit meta input ([#423](https://github.com/coolbress/VertexLab/issues/423)) ([ab69b1e](https://github.com/coolbress/VertexLab/commit/ab69b1ee88ed16511ffd58f84f23e4a8280fed0b))

## [0.19.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.19.0...vertex-forager-v0.19.1) (2026-04-01)


### Reverts

* **vertex-forager:** revert async SQLite wrapping from [#418](https://github.com/coolbress/VertexLab/issues/418) ([#420](https://github.com/coolbress/VertexLab/issues/420)) ([986d886](https://github.com/coolbress/VertexLab/commit/986d886f577a58cebfd7f9f7040fcf912aa826ea))

## [0.19.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.18.0...vertex-forager-v0.19.0) (2026-04-01)


### Features

* async queue creation and thread-safe history save ([#418](https://github.com/coolbress/VertexLab/issues/418)) ([4213112](https://github.com/coolbress/VertexLab/commit/42131121e59872373f001ff3ecf82c650f9ce307))

## [0.18.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.17.0...vertex-forager-v0.18.0) (2026-04-01)


### ⚠ BREAKING CHANGES

* **vertex-forager:** redesign progress reporting API ([#414](https://github.com/coolbress/VertexLab/issues/414))

### Features

* **vertex-forager:** redesign progress reporting API ([#414](https://github.com/coolbress/VertexLab/issues/414)) ([be86dcf](https://github.com/coolbress/VertexLab/commit/be86dcf1276357232de013d7d1caf398ee1c91d4))

## [0.17.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.16.0...vertex-forager-v0.17.0) (2026-04-01)


### ⚠ BREAKING CHANGES

* **vertex-forager:** replace pagination_max_burst with SchedulerConfig for always-on DRR ([#412](https://github.com/coolbress/VertexLab/issues/412))

### Features

* **vertex-forager:** replace pagination_max_burst with SchedulerConfig for always-on DRR ([#412](https://github.com/coolbress/VertexLab/issues/412)) ([4988fc3](https://github.com/coolbress/VertexLab/commit/4988fc3f56f9e8726556b507eccaa996d84d83e7))

## [0.16.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.15.1...vertex-forager-v0.16.0) (2026-04-01)


### Features

* **vertex-forager:** implement pagination fairness demotion ([#409](https://github.com/coolbress/VertexLab/issues/409)) ([363034a](https://github.com/coolbress/VertexLab/commit/363034a332d5518a77a43f732142c6cc758e7388))

## [0.15.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.15.0...vertex-forager-v0.15.1) (2026-03-31)


### Bug Fixes

* **vertex-forager:** update contributing link in testing.md to GitHub URL ([#406](https://github.com/coolbress/VertexLab/issues/406)) ([aba66f4](https://github.com/coolbress/VertexLab/commit/aba66f4c5194432b1f7a83406b981fdd585bd098))

## [0.15.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.14.1...vertex-forager-v0.15.0) (2026-03-31)


### ⚠ BREAKING CHANGES

* migrate package into workspace path ([#404](https://github.com/coolbress/VertexLab/issues/404))

### Code Refactoring

* migrate package into workspace path ([#404](https://github.com/coolbress/VertexLab/issues/404)) ([0f46c04](https://github.com/coolbress/VertexLab/commit/0f46c04f4d3aa26ee79b590f9db283adec826466))

## [0.14.1](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.14.0...vertex-forager-v0.14.1) (2026-03-31)


### Documentation

* **vertex-forager:** fix mkdocs adaptive throttle references ([#402](https://github.com/coolbress/VertexLab/issues/402)) ([78bb168](https://github.com/coolbress/VertexLab/commit/78bb168e59ce43ed9159c56452fcc3cbd7f66250))

## [0.14.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.13.0...vertex-forager-v0.14.0) (2026-03-31)


### ⚠ BREAKING CHANGES

* **vertex-forager:** rename DownshiftConfig to AdaptiveThrottleConfig with ratio-based rpm floor and recovery ([#395](https://github.com/coolbress/VertexLab/issues/395))

### Bug Fixes

* **vertex-forager:** rename DownshiftConfig to AdaptiveThrottleConfig with ratio-based rpm floor and recovery ([#395](https://github.com/coolbress/VertexLab/issues/395)) ([0b7ce22](https://github.com/coolbress/VertexLab/commit/0b7ce222406491fff6be348a3dd947b2bd976ccd))

## [0.13.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.12.0...vertex-forager-v0.13.0) (2026-03-31)


### ⚠ BREAKING CHANGES

* **vertex-forager:** remove enable_http_status_retry, migrate to retry_status_codes ([#391](https://github.com/coolbress/VertexLab/issues/391))

## [0.12.0](https://github.com/coolbress/VertexLab/compare/vertex-forager-v0.11.7...vertex-forager-v0.12.0) (2026-03-31)


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
