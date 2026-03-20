from __future__ import annotations

import polars as pl
import pytest

from vertex_forager.writers.memory import InMemoryBufferWriter


@pytest.mark.asyncio
async def test_inmemory_writer_dedup_with_unique_key(pkt_factory) -> None:
    writer = InMemoryBufferWriter()
    writer.set_unique_key(["id"])

    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    df2 = pl.DataFrame({"id": [2, 3], "val": ["b2", "c"]})

    await writer.write(pkt_factory("tmp_table", df1))
    await writer.write(pkt_factory("tmp_table", df2))

    out = writer.collect_table("tmp_table")
    # Expect ids {1,2,3} with id=2 keeping last value "b2"
    assert sorted(out["id"].to_list()) == [1, 2, 3]
    assert out.filter(pl.col("id") == 2)["val"][0] == "b2"

    counters = writer.get_counters_and_reset()
    assert counters.get("inmem_dedup_dropped_rows", 0) == 1
