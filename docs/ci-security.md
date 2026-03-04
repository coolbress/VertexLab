# CI Security and Quality Triage Guide

## Branch Protection
- Enable Branch Protection on the `main` branch.
- Require status checks: `Vertex CI / 🛡️ Code Quality Check`, `Type Check / mypy`, `CodeQL / analyze`, `CodeRabbit` (exact check names as shown in PR).
- Disallow direct pushes and require PR reviews.

### Setup Steps (GitHub UI)
1. Go to Settings → Branches → Branch protection rules → Add rule
2. Branch name pattern: `main`
3. Enable:
   - Require a pull request before merging
   - Require status checks to pass before merging
   - Require conversation resolution before merging
4. Required status checks (select exact names as they appear in PR checks list):
   - `Vertex CI / 🛡️ Code Quality Check`
   - `Type Check / mypy`
   - `CodeQL / analyze`
   - `CodeRabbit`
5. Optional hardening:
   - Require linear history
   - Require signed commits
   - Dismiss stale pull request approvals when new commits are pushed

## Workflows
- `quality-check`: Runs Trivy vulnerability scans (secret/config), import cycle/security checks, and the test suite as merge gates.
- `CodeQL`: Static analysis with SARIF results published to the Security tab (public repo, uploads enabled).

## 현재 상태
- 저장소: 공개(public) 레포지토리
- 브랜치 보호: `main`에 활성화, 필수 체크는 `Vertex CI / 🛡️ Code Quality Check`, `Type Check / mypy`, `CodeQL / analyze`, `CodeRabbit`
- 병합 정책: Auto‑merge 비활성, Squash and merge 허용, 머지 후 브랜치 자동 삭제
- 보안 탭: CodeQL/Trivy SARIF 결과 게시 및 집계

## 목표 상태 / 예외 및 참고
- 예외(계획/환경 제약): 사설 플랜 또는 코드 스캐닝 비활성 환경에서는 SARIF 게시가 제한될 수 있음
- 업로드 비활성 사례(`upload: false`): CodeQL이 실행되더라도 SARIF가 게시되지 않음. 게시를 원하면 업로드를 활성화
- 선택 사항: 협업/리뷰 정책 강화를 원하면 승인(review) 요구 또는 Code Owners 리뷰 요구를 추가로 구성

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
- 브랜치 보호가 활성화된 상태에서 필수 체크 실패 시 PR 병합 차단
- 로그에 remediation 힌트가 포함되어 원인 파악 및 조치가 용이

## Performance
- Lint checks kept minimal to reduce runtime.
- CodeQL runs on push/PR to `main`, and weekly scheduled scans are enabled.
