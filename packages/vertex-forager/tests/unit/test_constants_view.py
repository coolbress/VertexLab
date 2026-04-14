from __future__ import annotations

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
