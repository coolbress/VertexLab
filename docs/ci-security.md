# CI Security Policy

## Code Scanning

- CodeQL analyzes Python code on pushes and PRs.
- Upload on PRs is enabled when either repository variable `ENABLE_CODEQL_ON_PR == true` is set or the PR has label `security-critical`. Pushes to `main` always upload.

## Vulnerability Scans (Trivy)

- Two-step flow:
  - SARIF step: runs with severities `MEDIUM,HIGH,CRITICAL`, produces `trivy-results.sarif`, and does not fail the build so the Security tab captures all severities.
  - Gate step: runs with `HIGH,CRITICAL` only and fails the job to enforce the CI gate; reuses Trivy setup/cache.
- Remediation hints are printed on failure; see severity thresholds and SLA below.

## Severity Policy and SLA

- HIGH, CRITICAL: block merges; fix promptly.
- MEDIUM: reported to Security tab; fix when feasible; does not block.
- LOW, UNKNOWN: informational; track in backlog.

## Publishing Gate

- PyPI upload requires `vars.PUBLISH_TO_PYPI == 'true'` and `secrets.PYPI_TOKEN`.
- No other steps use PyPI credentials.

## Runtime

- Workflows use Node 24‑compatible action versions; Python 3.10 runners.

## Rollback

- Toggle CodeQL PR uploads via `ENABLE_CODEQL_ON_PR`.
- Disable Trivy gate temporarily by removing the gate step or adjusting severity.
- Revert specific workflow files via Git history.
