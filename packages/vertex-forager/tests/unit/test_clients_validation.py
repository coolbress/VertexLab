from vertex_forager.clients.validation import filter_reserved_kwargs


def test_filter_reserved_kwargs_removes_reserved_keys():
    kwargs = {
        "router": 1, "dataset": 2, "custom": 3, "symbols": 4, "extra": 5,
        "writer": 6, "mapper": 7, "on_progress": 8,
    }
    out = filter_reserved_kwargs(
        kwargs, {"router", "dataset", "symbols", "writer", "mapper", "on_progress"}
    )
    assert "router" not in out
    assert "dataset" not in out
    assert "symbols" not in out
    assert "writer" not in out
    assert "mapper" not in out
    assert "on_progress" not in out
    assert out["custom"] == 3
    assert out["extra"] == 5
