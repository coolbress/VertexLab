from pathlib import Path

import pytest

pytestmark = pytest.mark.manual


@pytest.mark.parametrize(
    "path",
    [
        Path(__file__).parent / "verification" / "verify_core_perf_budget.py",
        Path(__file__).parent / "verification" / "verify_examples_provider_matrix.py",
        Path(__file__).parent / "verification" / "verify_pipeline_chunked_memory.py",
        Path(__file__).parent / "verification" / "verify_pipeline_perf.py",
        Path(__file__).parent / "verification" / "verify_pipeline_sweep.py",
        Path(__file__).parent / "verification" / "verify_duckdb_write_strategy.py",
        Path(__file__).parent / "verification" / "verify_scheduler_fairness.py",
    ],
)
def test_manual_scripts_exist(path: Path) -> None:
    assert path.exists()
