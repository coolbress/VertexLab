from vertex_forager.clients.validation import filter_reserved_kwargs


def test_filter_reserved_kwargs_removes_reserved_keys():
    kwargs = {"router": 1, "dataset": 2, "custom": 3, "symbols": 4, "extra": 5}
    out = filter_reserved_kwargs(kwargs, {"router", "dataset", "symbols"})
    assert "router" not in out and "dataset" not in out and "symbols" not in out
    assert out["custom"] == 3 and out["extra"] == 5
