# CI Security Policy

## Code Scanning
- CodeQL analyzes Python code on pushes and PRs.
- Upload on PRs is controlled by repository variable `ENABLE_CODEQL_ON_PR` (`true` to enable).

## Vulnerability Scans (Trivy)
- SARIF upload: runs on main pushes; severities: `MEDIUM,HIGH,CRITICAL`; uploads to Security tab.
- Gate (table): fails CI only on `HIGH,CRITICAL`; ignores unfixed issues and common caches.
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
