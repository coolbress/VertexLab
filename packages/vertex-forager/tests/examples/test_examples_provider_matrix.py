from __future__ import annotations

import os
import importlib.util
from pathlib import Path
import pytest


def _import_from_file(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


PROVIDERS = [p.strip() for p in (os.getenv("VF_EXAMPLES_PROVIDER") or "yfinance").split(",") if p.strip()]


@pytest.mark.skipif(os.getenv("VF_EXAMPLES_SMOKE") != "1", reason="Set VF_EXAMPLES_SMOKE=1 to run example smoke tests")
@pytest.mark.parametrize("provider", PROVIDERS)
def test_minimal_inmemory_env_driven(provider: str) -> None:
    if provider == "sharadar" and not os.getenv("SHARADAR_API_KEY"):
        pytest.skip("Requires SHARADAR_API_KEY to run Sharadar example")
    root = Path(__file__).resolve().parents[4]
    example = root / "packages" / "vertex-forager" / "examples" / "minimal_inmemory.py"
    mod = _import_from_file(example)
    os.environ["VF_PROVIDER"] = provider
    os.environ.setdefault("VF_TICKERS", "AAPL,MSFT")
    assert hasattr(mod, "main")
    mod.main()  # type: ignore[attr-defined]


@pytest.mark.skipif(os.getenv("VF_EXAMPLES_SMOKE") != "1", reason="Set VF_EXAMPLES_SMOKE=1 to run example smoke tests")
@pytest.mark.parametrize("provider", PROVIDERS)
def test_advanced_duckdb_runs(provider: str, tmp_path) -> None:
    if provider == "sharadar" and not os.getenv("SHARADAR_API_KEY"):
        pytest.skip("Requires SHARADAR_API_KEY for sharadar")
    root = Path(__file__).resolve().parents[4]
    example = root / "packages" / "vertex-forager" / "examples" / "advanced_duckdb_metrics.py"
    mod = _import_from_file(example)
    db = tmp_path / "forager.duckdb"
    os.environ["VF_PROVIDER"] = provider
    os.environ["VF_TICKERS"] = "AAPL,MSFT"
    os.environ["VF_DUCKDB_PATH"] = str(db)
    assert hasattr(mod, "main")
    mod.main()  # type: ignore[attr-defined]
    assert db.exists()
