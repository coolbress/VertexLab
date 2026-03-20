# CI Security Policy

## Code Scanning

- CodeQL analyzes Python code on pushes and PRs.
- Upload on PRs is enabled when either repository variable `ENABLE_CODEQL_ON_PR == true` is set or the PR has label `security-critical`. Pushes to `main` always upload.

## Vulnerability Scans (Trivy)

 - Single-run flow:
   - One Trivy execution runs with severities `MEDIUM,HIGH,CRITICAL`, produces `trivy-results.sarif`, and uses `exit-code: 0` so the Security tab captures all severities.
   - In the same job run, a SARIF-parsing gate enforces failure only when `HIGH` or `CRITICAL` findings are present (based on parsing the `trivy-results.sarif`), reusing setup/cache within that single execution.
- Remediation hints are printed on failure; see severity thresholds and SLA below.

## Severity Policy and SLA

- HIGH, CRITICAL: block merges; fix promptly.
- MEDIUM: reported to Security tab; fix when feasible; does not block.
- LOW, UNKNOWN: informational; track in backlog.

## Publishing Gate

- PyPI upload requires `vars.PUBLISH_TO_PYPI == 'true'` and `secrets.PYPI_TOKEN`.
- No other steps use PyPI credentials.

## Release PR CI Trigger (release-please)

- To ensure CI checks run automatically on Release PRs created by release-please, use a fine‑grained PAT:
  - Create a bot account or use an automation user with minimal scopes (Repository: Contents (R/W), Pull requests (R/W), Issues (R/W), Metadata (R/O)).
  - Add the token as repository secret: `RELEASE_PLEASE_TOKEN`.
  - The workflow uses `secrets.RELEASE_PLEASE_TOKEN` with fallback to `github.token`:
    - If the secret is present, PRs are authored via PAT and standard `pull_request` triggers fire.
    - If absent, the workflow falls back to `GITHUB_TOKEN` (PRs may not auto‑trigger CI due to chain restrictions).

## Runtime

- Workflows use Node 24‑compatible action versions; Python 3.10 runners.

## Rollback

- Toggle CodeQL PR uploads via `ENABLE_CODEQL_ON_PR`.
- Disable Trivy gate temporarily by removing the gate step or adjusting severity.
- Revert specific workflow files via Git history.
