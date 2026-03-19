import pytest

from vertex_forager.core.config import RequestSpec
from vertex_forager.core.library import (
    BaseLibraryFetcher,
    get_library_fetcher,
    register_library_fetcher,
)


class DummyFetcher(BaseLibraryFetcher):
    scheme = "dummy"
    def fetch(self, spec: RequestSpec):
        payload, dataset, lib = self.parse_spec(spec)
        t = lib.get("type")
        if t == "download":
            return {"payload": payload, "dataset": dataset, "ok": True}
        if t == "ticker_attr":
            attr = lib.get("attr")
            if not isinstance(attr, str) or attr.startswith("_") or "__" in attr:
                raise ValueError("Invalid attribute")
            return {"payload": payload, "attr": attr, "ok": True}
        raise ValueError("Unsupported type")


def test_registry_idempotent():
    existing = get_library_fetcher("dummy")
    if existing is None:
        register_library_fetcher(DummyFetcher())
    with pytest.raises(ValueError, match=r".*"):
        register_library_fetcher(DummyFetcher())


def test_parse_spec_validation():
    f = DummyFetcher()
    with pytest.raises(ValueError, match=r".*"):
        f.parse_spec(RequestSpec(url="dummy://AAPL", params={"dataset": "price"}))
    with pytest.raises(ValueError, match=r".*"):
        f.parse_spec(
            RequestSpec(url="other://AAPL", params={"lib": {"type": "download"}})
        )
