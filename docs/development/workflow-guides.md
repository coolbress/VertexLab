# Development Workflow Guides

This page consolidates practical workflow guidance previously kept under archived `legacy_code/fmp/docs/`.

- Architecture rationale is tracked in [ADR index](../adr/README.md).

## Repository Workflow

- Branching and PR policy: [CONTRIBUTING.md](https://github.com/coolbress/VertexLab/blob/main/CONTRIBUTING.md)
- PR template requirements: [.github/PULL_REQUEST_TEMPLATE.md](https://github.com/coolbress/VertexLab/blob/main/.github/PULL_REQUEST_TEMPLATE.md)
- PR automation behavior: [pr-automation.yml](../../.github/workflows/pr-automation.yml)

## CI and Release Operations

- CI security policy: [docs/ci-security.md](../ci-security.md)
- Node runtime policy for actions: [docs/explanation/ci-node-policy.md](../explanation/ci-node-policy.md)
- Labels and automation policy: [docs/explanation/labels-policy.md](../explanation/labels-policy.md)
- Main CI pipeline: [.github/workflows/ci.yml](https://github.com/coolbress/VertexLab/blob/main/.github/workflows/ci.yml)
- Release pipeline: [.github/workflows/release-please.yml](https://github.com/coolbress/VertexLab/blob/main/.github/workflows/release-please.yml)

## Package Development Tooling

- Workspace/build/test/lint commands: [CONTRIBUTING.md](https://github.com/coolbress/VertexLab/blob/main/CONTRIBUTING.md)
- Package docs entrypoint: [packages/vertex-forager/docs/index.md](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/docs/index.md)
