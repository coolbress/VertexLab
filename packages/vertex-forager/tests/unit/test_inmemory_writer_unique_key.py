from __future__ import annotations

import polars as pl
import pytest

from vertex_forager.writers.memory import InMemoryBufferWriter


@pytest.mark.asyncio
async def test_inmemory_writer_dedup_with_upsert_keys(pkt_factory) -> None:
    writer = InMemoryBufferWriter(upsert_keys=["id"])

    df1 = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    df2 = pl.DataFrame({"id": [2, 3], "val": ["b2", "c"]})

    await writer.write(pkt_factory("tmp_table", df1))
    await writer.write(pkt_factory("tmp_table", df2))

    # Expect inline dedup: total height should be 3, not 4
    assert len(writer._tables["tmp_table"]) == 1
    out = writer._tables["tmp_table"][0]

    assert sorted(out["id"].to_list()) == [1, 2, 3]
    assert out.filter(pl.col("id") == 2)["val"][0] == "b2"

    counters = writer.get_counters_and_reset()
    assert counters.get("inmem_dedup_dropped_rows", 0) == 1


@pytest.mark.asyncio
async def test_inmemory_writer_dedup_is_deterministic_across_packet_order(pkt_factory) -> None:
    writer1 = InMemoryBufferWriter(upsert_keys=["id"])
    writer2 = InMemoryBufferWriter(upsert_keys=["id"])

    df1 = pl.DataFrame({"id": [1], "val": ["a"]})
    df2 = pl.DataFrame({"id": [1], "val": ["b"]})

    await writer1.write(pkt_factory("tmp_table", df1))
    await writer1.write(pkt_factory("tmp_table", df2))

    await writer2.write(pkt_factory("tmp_table", df2))
    await writer2.write(pkt_factory("tmp_table", df1))

    out1 = writer1.collect_table("tmp_table")
    out2 = writer2.collect_table("tmp_table")

    assert out1.equals(out2)
    assert out1["val"][0] == "b"
