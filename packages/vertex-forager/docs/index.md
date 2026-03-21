# Vertex Forager Documentation

Industry‑grade ingestion for financial data: transport decoupling, schema‑aware normalization with Polars, and resilient writing with DLQ controls.

## Get Started

- Tutorials
  - [Quickstart](tutorials/quickstart.md)
- How‑to Guides
  - [Operate with DLQ disabled](how-to/dlq-disabled.md)
  - [Tune chunked flush thresholds](how-to/chunked-flush.md)
  - [Troubleshooting](how-to/troubleshooting.md)
  - [CLI equivalents](how-to/cli-equivalents.md)
- Reference
  - [API Reference](reference/api.md)
  - [Module Index](reference/modules.md)
  - [EngineConfig](reference/config.md)
  - [Metrics](reference/metrics.md)
  - [Constants](reference/constants.md)
- Examples
  - [YFinance notebook](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/examples/yfinance_examples.ipynb)
  - [Sharadar notebook](https://github.com/coolbress/VertexLab/blob/main/packages/vertex-forager/examples/sharadar.ipynb)
- Explanation
  - [Pipeline architecture](explanation/architecture.md)
  - [Router & Client architecture](explanation/router-client.md)
  - [Data storage flow & DLQ](explanation/data-storage-flow.md)
  - [Writer security](explanation/writer-security.md)
  - [Writer fan‑out roadmap](explanation/writer-fanout-roadmap.md)
  - Diagrams: [pipeline_architecture.mmd](diagrams/pipeline_architecture.mmd)

## Principles

- Separate user needs by the Diátaxis quadrants (tutorials, how‑to, reference, explanation).
- Favor hands‑on quickstarts for first‑run success, precise references for API facts, and focused how‑tos for real tasks.

## Repository Docs (root)

- CI & Security
  - [CI Security](https://github.com/coolbress/VertexLab/blob/main/docs/ci-security.md)

## Links

- [README](https://github.com/coolbress/VertexLab/blob/main/README.md)
- [Changelog (Keep a Changelog)](https://github.com/coolbress/VertexLab/blob/main/CHANGELOG.md)
- [Semantic Versioning](https://semver.org/)
