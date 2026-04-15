from __future__ import annotations

import pytest

import vertex_forager.constants_view as constants_view
from vertex_forager.constants_view import build_constants_preview


def test_build_constants_preview_all_includes_provider_sections(monkeypatch) -> None:
    monkeypatch.setenv("SHARADAR_API_KEY", "secret")

    preview = build_constants_preview("all")

    assert "global" in preview
    assert "flow" in preview
    assert "queue" in preview
    assert "writers" in preview
    assert preview["yfinance"]["PRICE_BATCH_SIZE"] > 0
    assert preview["sharadar"]["MAX_ROWS_PER_REQUEST"] > 0
    assert preview["env_overrides"]["SHARADAR_API_KEY"] == "<redacted>"


def test_build_constants_preview_global_does_not_import_provider_preview(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail() -> dict[str, dict[str, object]]:
        raise AssertionError("provider preview should not be loaded for global-only section")

    monkeypatch.setattr(constants_view, "get_provider_constants_preview", _fail)

    preview = build_constants_preview("global")

    assert set(preview) == {"global"}
