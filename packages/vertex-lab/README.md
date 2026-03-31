# vertex-lab

Thin orchestration package and CLI shell for the VertexLab workspace.

## Purpose

- Provide the user-facing `vertex-lab` install target
- Coordinate `vertex-forager`, `vertex-qt`, and `vertex-workspace`
- Host future CLI/app entrypoints without mixing package ownership into the workspace root

## Current status

The package is present as a thin wrapper today. The CLI entrypoint is intentionally stubbed until the first real app workflow lands.
