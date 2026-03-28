# Vertex Lab · Claude Code CLI Guardrails

This document defines repository-level guardrails for using Claude Code CLI in VertexLab. It emphasizes token efficiency, safe minimal changes, industry-grade workflow discipline, and repository-specific quality and security expectations.

## Project Overview

- VertexLab is a Python 3.10+ monorepo managed with `uv`.
- The repository contains shared docs, CI/release automation, and multiple packages under `packages/`.
- Current package landscape:
  - `packages/vertex-forager/` for data ingestion, normalization, and persistence workflows
  - `packages/vertex-qt/` for quantitative analysis components
  - `packages/vertex-workspace/` for workspace and dashboard integration
- Root-level repository concerns live in:
  - `docs/` for repository docs and ADRs
  - `.github/workflows/` for CI, release, and automation
  - `scripts/` for verification helpers such as cycle checks

## Setup

- From the repository root:

  ```bash
  uv sync --group dev
  pre-commit install
  ```

- Use repository-root-relative paths in commands unless a package-specific directory is required.
- Prefer syncing the environment before running docs, tests, or automation checks after branch switches or merges.

| Variable | Purpose | Notes |
| --- | --- | --- |
| `SHARADAR_API_KEY` | Enables Sharadar-backed workflows and examples | Never commit or print it |
| `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` | Temporary CI compatibility guard for GitHub Actions runtime | Used in workflows, not a user-facing runtime setting |
| `VF_PROFILE_OUTPUT_DIR` | Output directory for benchmark/profile artifacts | Used by verification scripts and CI benchmark jobs |
| `PUBLISH_TO_PYPI` | Enables publish behavior in release automation when configured | Repository variable, not a committed secret |

## Common Commands

| Task | Command |
| --- | --- |
| Sync environment | `uv sync --group dev` |
| Lint | `uv run ruff check packages/ --fix` |
| Security lint | `uv run ruff check packages/ --select S` |
| Type check | `uv run mypy packages/vertex-forager/src --strict` |
| Tests | `uv run pytest packages/ --cov-fail-under=80` |
| Smoke tests | `uv run pytest packages/ -m smoke` |
| Cycle check | `uv run python scripts/check_cycles.py` |
| Secrets scan | `uv run detect-secrets scan --all-files` |
| Benchmark profile | `uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf.py` |
| Docs build | `NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict` |

## Project Structure

```text
vertex-lab/
├── .github/                   # Workflows, issue templates, release automation
├── docs/                      # Root docs, policies, ADRs
├── packages/
│   ├── vertex-forager/        # Data ingestion package
│   ├── vertex-qt/             # Quant analysis package
│   └── vertex-workspace/      # Workspace/dashboard package
├── scripts/                   # Repository verification helpers
├── src/vertex_lab/            # Root meta-package
├── mkdocs.yml                 # Root docs navigation
├── pyproject.toml             # Workspace configuration
└── claude.md                  # Repository guardrails for Claude Code CLI
```

## Code Style & Quality

- Prefer existing style and dependencies; introduce or replace libraries only when they demonstrably improve performance, reliability, security, or maintainability and the dependency checklist is satisfied.
- Edit only code directly related to the requested feature or bug; avoid opportunistic refactors or renames.
- Follow PEP 8, Google-style docstrings, strict mypy, Pydantic for runtime validation, and `pathlib.Path`.
- Do not add comments in code unless explicitly requested.
- Evaluate changes across six areas: correctness, data integrity, security, performance, reliability, and code quality.
- Follow SOLID and protocol-based abstractions for replaceable implementations.
- Resource guidelines:
  - Network: use an async client, manage connection pools, implement exponential backoff retries
  - Data: prefer vectorized operations and memory-efficient engines based on dataset scale
  - Concurrency: respect rate limits and system constraints; avoid event-loop blocking

### Dependency Change Checklist

- Issue opened and linked with clear rationale and scope.
- Benchmarks or profiling show material improvement on representative workloads.
- Security and license review completed; supply-chain risks considered.
- API stability and ecosystem maturity evaluated.
- Minimal surface-area change with migration and rollback plan.
- Manage with `uv` from the package directory and pin versions with appropriate bounds:
  - Core/runtime dependencies: apply upper bounds per repo policy
  - Dev tools: lower-bound pins are acceptable
- All quality gates pass.

## Testing

- All quality gates below are mandatory and should be run from the repository root:
  - Lint: `uv run ruff check packages/ --fix`
  - Security lint: `uv run ruff check packages/ --select S`
  - Types: `uv run mypy packages/vertex-forager/src --strict`
  - Tests: `uv run pytest packages/ --cov-fail-under=80`
  - Cycles: `uv run python scripts/check_cycles.py`
  - Secrets scan: `uv run detect-secrets scan --all-files`
- Benchmarking and profiling policy:
  - Compare against context-appropriate industry references and document the rationale for chosen baselines
  - Track latency, memory footprint, and throughput
  - Demonstrate improvements through benchmarks or profiles when performance claims are made
- Internal profiling example:

  ```bash
  uv run python packages/vertex-forager/tests/verification/verify_pipeline_perf.py
  ```

- Benchmark artifacts are written to `tests/verification/profile_metrics.json` or the path configured by `VF_PROFILE_OUTPUT_DIR`.

## Git Workflow

- Issue creation:
  - Create issues in English with the appropriate GitHub template
  - Capture the issue number before implementing
- Branching:
  - Work on `feat/`, `fix/`, or `refactor/` branches with the issue number
  - Never push directly to `main`
- PRs:
  - Use `gh pr create`
  - PR bodies must contain: Summary, Linked Issue, Changes, Verification, Checklist
- Commits:
  - Use Conventional Commits: `<type>(<scope>)?: <desc>`
  - PR titles use `type(scope)?: summary`
  - Link issues in the PR body instead of appending issue numbers to the title
  - Allowed types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`
  - Squash merge uses the PR title as the final commit message
- Pre-change safety checks:
  - `git fetch origin`
  - `git diff origin/main -- <file>`
- Directory deletion safeguards:
  - Resolve paths and ensure the target stays under the repository root
  - Protect root and home directories; verify existence and directory type before deletion
- `uv.lock` conflicts:
  - Regenerate deterministically from the repository root with `rm uv.lock && uv lock`
  - `uv sync` is not required for lockfile merge conflict resolution
- Post-merge environment sync:
  - `uv sync`
  - `uv sync --group dev`

## Security

- Never include secrets, API keys, tokens, or credentials in `claude.md`, code, docs, or logs.
- Manage sensitive configuration with environment variables; do not commit `.env` files.
- Avoid printing or storing secrets; scrub logs if accidental exposure occurs.
- Document only the principle of secret handling, not concrete secret values.
- Consider supply-chain risk whenever adding dependencies or downloading external tooling in CI.

## Claude-Specific Behavior

### Output Contract

- Language: general chat in Korean; code comments if needed, issues, PRs, and commit messages in English.
- Format for all assistant outputs:
  1. Conclusion
  2. Related file links in `file:///ABS/PATH#Lx-Ly` form
  3. Next actions
- Example link: [api.py](file:///$REPO_ROOT/packages/vertex-forager/src/vertex_forager/api.py#L10-L35)
- Links must use absolute `file:///` URLs.
- Commands should be executable from the repository root unless explicitly documented otherwise.

### Exploration Strategy

- No recursive listing such as `ls -R`.
- Identify target files first, then read only the required line ranges.
- Never load entire long files unless unavoidable; prefer precise ranges.
- Start with high-level questions, then split into focused sub-queries.

### Analysis Ignore Policy

- `.claudeignore` complements `.gitignore` to exclude large non-code artifacts such as datasets, logs, reports, and media from AI analysis.
- Do not exclude source code needed for understanding or refactoring.
- Keep ignore rules minimal, repository-wide, and easy to audit.
