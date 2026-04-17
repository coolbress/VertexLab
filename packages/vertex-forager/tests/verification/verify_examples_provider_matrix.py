from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.manual


def _import_from_file(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


ROOT = Path(__file__).resolve().parents[4]
EXAMPLES_DIR = ROOT / "packages" / "vertex-forager" / "examples"
RUNNABLE_EXAMPLES = sorted(path for path in EXAMPLES_DIR.glob("*.py"))
PROVIDERS = [p.strip() for p in (os.getenv("VF_EXAMPLES_PROVIDER") or "yfinance").split(",") if p.strip()]


def _required_env_vars(provider: str) -> list[str]:
    if provider == "sharadar":
        return ["SHARADAR_API_KEY"]
    return []


def _skip_if_provider_unavailable(provider: str) -> None:
    missing = [name for name in _required_env_vars(provider) if not os.getenv(name)]
    if missing:
        pytest.skip(f"Requires {', '.join(missing)} for provider={provider}")


@pytest.mark.skipif(os.getenv("VF_EXAMPLES_SMOKE") != "1", reason="Set VF_EXAMPLES_SMOKE=1 to run example smoke tests")
@pytest.mark.parametrize("example_path", RUNNABLE_EXAMPLES, ids=lambda path: path.name)
@pytest.mark.parametrize("provider", PROVIDERS)
def test_examples_provider_matrix(
    example_path: Path,
    provider: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _skip_if_provider_unavailable(provider)
    mod = _import_from_file(example_path)
    db = tmp_path / "forager.duckdb"
    monkeypatch.setenv("VF_PROVIDER", provider)
    monkeypatch.setenv("VF_TICKERS", "AAPL,MSFT")
    monkeypatch.setenv("VF_DUCKDB_PATH", str(db))
    assert hasattr(mod, "main")
    mod.main()  # type: ignore[attr-defined]
    if example_path.name == "advanced_duckdb_metrics.py":
        assert db.exists()
