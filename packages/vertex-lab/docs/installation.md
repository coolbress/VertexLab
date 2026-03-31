# Installation

Install `vertex-lab` when you want the top-level VertexLab app package and a single user-facing entrypoint for the workspace.

## From the repository

```bash
git clone https://github.com/coolbress/VertexLab.git vertex-lab
cd vertex-lab
uv sync --group dev
```

## Package boundary

The `vertex-lab` package now lives in `packages/vertex-lab/` and is released independently from the repository root.

Until the first real app workflow ships, package-local installation is mainly useful for development and release verification.
