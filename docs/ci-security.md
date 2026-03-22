# CI Security & Publishing

## Objectives
- Eliminate long‑lived PyPI tokens via Trusted Publishing (OIDC).
- Enforce Conventional Commits at PR title level (warning first).

## Trusted Publishing (PyPI)
- Publisher: pypa/gh-action-pypi-publish@release/v1
- Job permissions: contents: write, id-token: write
- Toggle: set repository variable `PUBLISH_TO_PYPI=true` to enable
- Build artifacts path: `dist/vertex-forager`
- Registration: add this repo as a Trusted Publisher on PyPI
- Rollback: switch back to Twine + PYPI_TOKEN

## PR Title Validation
- Trigger: pull_request (opened, edited, synchronize, reopened, ready_for_review)
- Pattern (Conventional Commits + issue):  
  `^(build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test)(\([^)]+\))?(!)?: .+ \(#\d+\)$`
- Mode: Warning only (`continue-on-error: true`), later switch to blocking

## Validation
- Publish uses OIDC and succeeds without PYPI_TOKEN
- Non‑conforming PR titles produce a warning
- Release Please changelog unaffected

## Risk & Rollback
- Trusted Publishing: requires PyPI setup; if misconfigured, publish fails only
- Commit validation: regex may be strict; start warning → then blocking

## References
- `.github/workflows/release-please.yml`
- `.github/workflows/validate-pr-title.yml`
