# CI Node Runtime Policy

This page documents the policy for JavaScript‑based GitHub Actions in this repository:

- Node runtime: unify on Node 24 for all actions
- Supply‑chain safety: pin actions by SHA; avoid floating tags
- Compatibility guard: `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` environment variable may be present in select jobs as a safety net; once all actions are confirmed Node 24‑compatible and SHA‑pinned, this guard can be deprecated

## Rationale

- Node 24 provides a stable baseline with long‑term support and improved performance
- SHA‑pinned actions ensure reproducibility and reduce re‑pointing risks
- A temporary guard variable avoids accidental downgrades when upstream changes action defaults

## References

- CI workflow: https://github.com/coolbress/VertexLab/blob/main/.github/workflows/ci.yml
- Type Check workflow: https://github.com/coolbress/VertexLab/blob/main/.github/workflows/typecheck.yml
- Docs build/deploy: https://github.com/coolbress/VertexLab/blob/main/.github/workflows/docs.yml
- Labeler: https://github.com/coolbress/VertexLab/blob/main/.github/workflows/labeler.yml

## Policy Notes

- Prefer explicit Node runtime via action versions known to target Node 24
- Keep comments indicating upstream major (e.g., “# v5 pinned”) near `uses:` lines
- Deprecate `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` when auditing confirms all actions comply
