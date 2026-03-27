# VertexLab

VertexLab is a Python monorepo for data and research workflows, package development, and release automation across the Vertex toolchain.

## What You Will Find Here

- A root documentation portal for repository-wide workflows, policies, and architecture decisions
- Component documentation for packages maintained in this monorepo
- Contributor guidance, release process notes, and changelog history for the `vertex-lab` meta-package

## Components

### vertex-forager

`vertex-forager` is the current primary package in this repository. It provides data-ingestion and persistence workflows together with API reference, tutorials, how-to guides, and architecture explanations.

- Package docs: [vertex-forager](https://coolbress.github.io/VertexLab/vertex-forager/)
- Examples: [examples directory](https://github.com/coolbress/VertexLab/tree/main/packages/vertex-forager/examples)

## Start Here

- New to the repository: start with [Getting Started](getting-started.md)
- Working on changes: read [Contributing](contributing.md)
- Looking for release history: open [CHANGELOG.md](https://github.com/coolbress/VertexLab/blob/main/CHANGELOG.md)
- Reviewing technical decisions: browse the [ADR index](adr/README.md)

## Policies And Governance

- [CI Security](ci-security.md)
- [CI Node Runtime Policy](explanation/ci-node-policy.md)
- [Labels Policy](explanation/labels-policy.md)

## Repository Map

- `docs/` contains root policies, contributor guidance, and ADRs
- `packages/vertex-forager/` contains the main package source, tests, examples, and package docs
- `.github/workflows/` contains the CI, release, and automation workflows that govern the repository
