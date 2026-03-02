# Vertex Lab - Data Science Platform

Modern Python monorepo for financial data collection, analysis, and visualization.

## 🏗️ Architecture

```
vertex-lab/                    # Monorepo root
├── packages/                  # All packages
│   ├── vertex-forager/        # Data collection package
│   ├── vertex-caliber/        # Data analysis package  
│   └── vertex-viz/            # Data visualization package
├── .github/workflows/         # CI/CD pipelines
└── pyproject.toml            # Root dependencies & workspace config
```

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd vertex-lab

# Install dependencies
uv sync

# Install vertex-forager in development mode
uv add --editable packages/vertex-forager
```

### Development Workflow

1. **Create an Issue**: Plan what to build
2. **Create a Branch**: `git checkout -b feature/description`
3. **Develop**: Work in the appropriate package directory
4. **Test**: `uv run pytest packages/`
5. **Lint**: `uv run ruff check packages/ --fix`
6. **Commit**: `git commit -m "feat: description (#issue-number)"`
7. **Push & Create PR**: `git push -u origin feature/description`

## 📦 Packages

### vertex-forager
Data collection framework for financial markets.

```python
from vertex_forager.clients import create_client
from vertex_forager.core.models import FetchJob

client = create_client("sharadar")
job = FetchJob(
    dataset="price",
    symbols=["AAPL", "MSFT"],
    start_date="2024-01-01",
    end_date="2024-12-31"
)
result = client.fetch(job)
```

### vertex-caliber
Data analysis and quantitative research tools.

### vertex-viz
Interactive data visualization components.

## 🧪 Testing

```bash
# Run all tests
uv run pytest packages/ -v

# Run specific package tests
uv run pytest packages/vertex-forager/tests/ -v

# Run with coverage
uv run pytest packages/ --cov=packages/ --cov-report=html
```

## 🔧 Code Quality

```bash
# Lint all code
uv run ruff check packages/ --fix

# Format code
uv run ruff format packages/

# Type checking (if configured)
uv run mypy packages/
```

## 📁 Project Structure

Each package follows the modern Python `src` layout:

```
packages/vertex-forager/
├── src/
│   └── vertex_forager/
│       ├── clients/          # Data source clients
│       ├── connectors/       # API connectors
│       ├── core/            # Core interfaces & models
│       ├── schema/          # Data schemas
│       ├── writers/         # Data writers
│       └── __init__.py
├── tests/                   # Package tests
└── pyproject.toml          # Package dependencies
```

## 🤝 Contributing

1. Follow the [development workflow](#development-workflow)
2. Ensure all tests pass before creating PR
3. Include issue number in commit messages
4. Keep PRs focused and well-documented
5. Refer to docs for architecture and constants overview:
   - docs/router-client.md
   - docs/writer-security.md
   - docs/CONSTANTS.md

## 📄 License

MIT License - see LICENSE file for details.
