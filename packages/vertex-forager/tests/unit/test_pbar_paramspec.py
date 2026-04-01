from __future__ import annotations

from vertex_forager.core.config import ProgressSnapshot
from vertex_forager.utils import create_pbar_updater


class FakePbar:
    def __init__(self) -> None:
        self.updated = 0
        self.last_postfix: dict[str, object] | None = None
        self.closed = False

    def update(self, n: int) -> None:
        self.updated += n

    def set_postfix(self, **kwargs: object) -> None:
        self.last_postfix = kwargs

    def close(self) -> None:
        self.closed = True


def test_create_pbar_updater_accepts_arbitrary_signature() -> None:
    pbar = FakePbar()
    cb = create_pbar_updater(pbar)  # type: ignore[arg-type]
    cb(
        ProgressSnapshot(
            jobs_done=1,
            jobs_total=2,
            pct=50.0,
            throughput_sym_per_s=2.0,
            eta_s=1.0,
            errors=0,
            retries=0,
            rows_written=10,
            elapsed_s=1.0,
            active_workers=1,
            pending_jobs=1,
            throttle_events=0,
            dlq_spooled=0,
            memory_mb=100.0,
            cpu_pct=5.0,
        )
    )
    assert pbar.updated == 1
    assert pbar.last_postfix is not None

    cb(
        ProgressSnapshot(
            jobs_done=2,
            jobs_total=2,
            pct=100.0,
            throughput_sym_per_s=2.0,
            eta_s=0.0,
            errors=0,
            retries=0,
            rows_written=20,
            elapsed_s=2.0,
            active_workers=0,
            pending_jobs=0,
            throttle_events=0,
            dlq_spooled=0,
            memory_mb=101.0,
            cpu_pct=6.0,
            finished=True,
        )
    )
    assert pbar.updated == 2
    assert pbar.closed is True
