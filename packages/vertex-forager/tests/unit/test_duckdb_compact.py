from __future__ import annotations

from vertex_forager.writers.duckdb import DuckDBWriter


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, sql: str):
        self.executed.append(sql)
        return self

    def fetchall(self):
        return []

    def register(self, *_args, **_kwargs):
        return None

    def unregister(self, *_args, **_kwargs):
        return None


def test_duckdb_compact_executes_vacuum_and_checkpoint(tmp_path) -> None:
    writer = DuckDBWriter(tmp_path / "test.duckdb")
    fake = _FakeConn()
    # Inject fake connection to avoid touching a real DB
    writer.__dict__["_conn"] = fake
    # Run sync path directly
    writer._compact_sync()
    # Verify commands were attempted
    assert "VACUUM" in fake.executed
    # CHECKPOINT is attempted but may be guarded; ensure it was called at least once
    assert any(cmd.startswith("CHECKPOINT") or cmd == "CHECKPOINT" for cmd in fake.executed)
