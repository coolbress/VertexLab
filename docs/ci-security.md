# CI Security and Quality Triage Guide

## Branch Protection
- Enable Branch Protection on the `main` branch.
- Require status checks: `quality-check` and `CodeQL`.
- Disallow direct pushes and require PR reviews.

### Setup Steps (GitHub UI)
1. Go to Settings → Branches → Branch protection rules → Add rule
2. Branch name pattern: `main`
3. Enable:
   - Require a pull request before merging
   - Require status checks to pass before merging
   - Require conversation resolution before merging
4. Required status checks (select exact names as they appear in PR):
   - `Vertex CI / quality-check`
   - `Type Check / mypy`
   - `CodeQL / analyze`
5. Optional hardening:
   - Require linear history
   - Require signed commits
   - Dismiss stale pull request approvals when new commits are pushed

## Workflows
- `quality-check`: Runs fast lint checks to gate merges.
- `CodeQL`: Performs static analysis and publishes SARIF results.

## Severity Thresholds
- High/Critical: Must be fixed or explicitly waived with justification and owner.
- Medium: Fix in the next sprint or document mitigation.
- Low/Info: Fix opportunistically.

## False-Positive Policy
- Only suppress findings with clear justification and a link to evidence.
- Prefer configuration fixes over blanket ignores.

## Ownership and SLA
- Security findings owned by the code area maintainers.
- SLA: Critical ≤24h, High ≤72h, Medium ≤14d, Low ≤30d.

## Remediation Pointers
- CodeQL query details include guidance; follow recommended fixes.
- Secret/config issues: rotate credentials, remove from history, use env/CI secrets.
- Dependency issues: upgrade or apply vendor patches.

## Failure Signals
- PRs blocked when `quality-check` or `CodeQL` fail.
- Logs include brief remediation hints and links to findings.

## Performance
- Lint checks kept minimal to reduce runtime.
- CodeQL runs on push/PR to `main`; consider scheduled runs for deeper scans.
