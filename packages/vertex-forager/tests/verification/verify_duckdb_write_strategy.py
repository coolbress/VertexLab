from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import time

import duckdb
import polars as pl
import pytest

pytestmark = pytest.mark.manual


def _output_path() -> Path:
    out_dir_env = os.getenv("VF_PROFILE_OUTPUT_DIR")
    out_dir = Path(out_dir_env) if out_dir_env else (Path.cwd() / "output" / "forager-profiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "profile_write_strategy.json"


def _append_only_benchmark() -> dict[str, float]:
    conn = duckdb.connect(":memory:")
    try:
        rows = 10_000
        frame = pl.DataFrame(
            {
                "provider": ["test"] * rows,
                "ticker": [f"T{i}" for i in range(rows)],
                "date": [date(2024, 1, 1)] * rows,
                "close": [float(i) for i in range(rows)],
            }
        )
        conn.execute("CREATE TABLE t (provider VARCHAR, ticker VARCHAR, date DATE, close DOUBLE)")
        start = time.perf_counter()
        conn.register("append_df", frame)
        conn.execute("INSERT INTO t SELECT * FROM append_df")
        elapsed = time.perf_counter() - start
        inserted = int(conn.execute("SELECT count(*) FROM t").fetchone()[0])
        return {
            "rows": float(rows),
            "inserted_rows": float(inserted),
            "duration_s": elapsed,
            "rows_per_s": rows / elapsed if elapsed > 0 else 0.0,
        }
    finally:
        conn.close()


def _high_conflict_upsert_benchmark() -> dict[str, float]:
    conn = duckdb.connect(":memory:")
    try:
        rows = 1_000
        rewrites = 10
        base = pl.DataFrame(
            {
                "provider": ["test"] * rows,
                "ticker": [f"T{i}" for i in range(rows)],
                "date": [date(2024, 1, 1)] * rows,
                "close": [float(i) for i in range(rows)],
            }
        )
        conn.execute(
            "CREATE TABLE t ("
            "provider VARCHAR, ticker VARCHAR, date DATE, close DOUBLE, "
            "PRIMARY KEY(provider, ticker, date))"
        )

        start = time.perf_counter()
        for rewrite in range(rewrites):
            frame = base.with_columns((pl.col("close") + rewrite).alias("close"))
            conn.register("upsert_df", frame)
            conn.execute(
                """
                INSERT INTO t SELECT * FROM upsert_df
                ON CONFLICT(provider, ticker, date) DO UPDATE SET close = EXCLUDED.close
                """
            )
        upsert_elapsed = time.perf_counter() - start

        dedup_frame = (
            pl.concat(
                [
                    base.with_columns(
                        (pl.col("close") + rewrite).alias("close"),
                        pl.lit(rewrite).alias("_rewrite_order"),
                    )
                    for rewrite in range(rewrites)
                ],
                how="vertical",
            )
            .sort(["provider", "ticker", "date", "_rewrite_order"])
            .unique(
                subset=["provider", "ticker", "date"],
                keep="last",
                maintain_order=True,
            )
            .drop("_rewrite_order")
        )
        conn.execute("DELETE FROM t")
        start = time.perf_counter()
        conn.register("dedup_df", dedup_frame)
        conn.execute("INSERT INTO t SELECT * FROM dedup_df")
        dedup_elapsed = time.perf_counter() - start
        final_rows = int(conn.execute("SELECT count(*) FROM t").fetchone()[0])
        return {
            "rows": float(rows),
            "rewrites": float(rewrites),
            "upsert_duration_s": upsert_elapsed,
            "dedup_insert_duration_s": dedup_elapsed,
            "final_rows": float(final_rows),
        }
    finally:
        conn.close()


def _large_schema_benchmark() -> dict[str, float]:
    conn = duckdb.connect(":memory:")
    try:
        rows = 10_000
        columns = {f"c{i}": list(range(rows)) for i in range(50)}
        frame = pl.DataFrame(columns)
        conn.execute(
            "CREATE TABLE t ({cols})".format(
                cols=", ".join(f"c{i} BIGINT" for i in range(50)),
            )
        )
        start = time.perf_counter()
        concat_frame = pl.concat(
            [frame.slice(0, rows // 2), frame.slice(rows // 2, rows - (rows // 2))],
            how="vertical",
        )
        conn.register("concat_df", concat_frame)
        conn.execute("INSERT INTO t SELECT * FROM concat_df")
        concat_elapsed = time.perf_counter() - start

        conn.execute("DELETE FROM t")
        start = time.perf_counter()
        conn.register("direct_df", frame)
        conn.execute("INSERT INTO t SELECT * FROM direct_df")
        direct_elapsed = time.perf_counter() - start

        return {
            "rows": float(rows),
            "columns": 50.0,
            "concat_register_insert_s": concat_elapsed,
            "direct_register_insert_s": direct_elapsed,
        }
    finally:
        conn.close()


def run_write_strategy_profile() -> dict[str, dict[str, float]]:
    results = {
        "append_only": _append_only_benchmark(),
        "high_conflict_upsert": _high_conflict_upsert_benchmark(),
        "large_schema": _large_schema_benchmark(),
    }
    output_path = _output_path()
    output_path.write_text(json.dumps(results, indent=2))
    return results


def test_duckdb_write_strategy_profile() -> None:
    results = run_write_strategy_profile()
    assert _output_path().exists()
    assert results["append_only"]["inserted_rows"] == results["append_only"]["rows"]
    assert results["high_conflict_upsert"]["final_rows"] == results["high_conflict_upsert"]["rows"]


if __name__ == "__main__":
    run_write_strategy_profile()
