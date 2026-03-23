from pathlib import Path

import pytest

pytestmark = pytest.mark.manual


@pytest.mark.parametrize(
    "path",
    [
        Path(__file__).parent / "verification" / "verify_pipeline_perf.py",
        Path(__file__).parent / "verification" / "verify_pipeline_perf_financials.py",
        Path(__file__).parent / "verification" / "verify_pipeline_sweep.py",
        Path(__file__).parent / "verification" / "verify_duckdb_upsert.py",
    ],
)
def test_manual_scripts_exist(path: Path) -> None:
    assert path.exists()
