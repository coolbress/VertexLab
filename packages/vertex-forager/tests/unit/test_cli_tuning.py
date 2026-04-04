from __future__ import annotations

from pathlib import Path

import click
from click.testing import CliRunner
import pytest

from vertex_forager import cli as cli_mod


def test_build_sweep_combinations_defaults() -> None:
    combos = cli_mod._build_sweep_combinations(None, None, None, None, None, sample_count=3, sample_seed=42)
    assert isinstance(combos, list)
    assert len(combos) == 3
    assert all(isinstance(c, dict) for c in combos)
    assert all("client_config" not in c for c in combos)
    assert all("limits" in c for c in combos)
    assert all("concurrency" in c for c in combos)


def test_build_sweep_combinations_invalid_token_raises() -> None:
    with pytest.raises(click.BadParameter):
        cli_mod._build_sweep_combinations("1,2,x", None, None, None, None, sample_count=1, sample_seed=1)


def test_build_sweep_combinations_invalid_zero_raises() -> None:
    with pytest.raises(click.BadParameter):
        cli_mod._build_sweep_combinations("0", None, None, None, None, sample_count=1, sample_seed=1)


def test_tune_profile_price_writes_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyRun:
        def __init__(self) -> None:
            self.metrics_counters = {"ok": 1}
            self.metrics_histograms = {}
            self.metrics_summary = {}
            self.tables = {}
            self.errors = []

    class DummyYF:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_price_data(self, **kwargs):  # pragma: no cover
            return DummyRun()

    # Patch client factory and writer
    import vertex_forager.providers.yfinance.client as yf_client

    monkeypatch.setattr(yf_client, "YFinanceClient", DummyYF, raising=True)
    captured = {}

    def _capture(path: Path, payload: object) -> None:
        captured["path"] = path
        captured["payload"] = payload

    monkeypatch.setattr(cli_mod, "_atomic_write_json", _capture, raising=True)

    out = tmp_path / "out"
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["tune", "profile", "--kind", "price", "--output-dir", str(out)])
    assert res.exit_code == 0
    assert "profile_metrics.json" in res.output
    assert captured
    assert "counters" in captured["payload"]


def test_tune_profile_financials_writes_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyRun:
        def __init__(self) -> None:
            self.metrics_counters = {"ok": 1}
            self.metrics_histograms = {}
            self.metrics_summary = {}
            self.tables = {}
            self.errors = []

    class DummyYF:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_financials(self, **kwargs):  # pragma: no cover
            return DummyRun()

    import vertex_forager.providers.yfinance.client as yf_client

    monkeypatch.setattr(yf_client, "YFinanceClient", DummyYF, raising=True)
    monkeypatch.delenv("SHARADAR_API_KEY", raising=False)
    captured = {}

    def _capture(path: Path, payload: object) -> None:
        captured["path"] = path
        captured["payload"] = payload

    monkeypatch.setattr(cli_mod, "_atomic_write_json", _capture, raising=True)
    out = tmp_path / "out2"
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["tune", "profile", "--kind", "financials", "--output-dir", str(out)])
    assert res.exit_code == 0
    assert "profile_financials_metrics.json" in res.output
    assert captured
    assert "yfinance_financials" in captured["payload"]


def test_tune_profile_financials_prefers_cli_tickers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class DummyRun:
        def __init__(self) -> None:
            self.metrics_counters = {}
            self.metrics_histograms = {}
            self.metrics_summary = {}
            self.tables = {}
            self.errors = []

    class DummyYF:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_financials(self, **kwargs):  # pragma: no cover
            captured["tickers"] = kwargs["tickers"]
            return DummyRun()

    import vertex_forager.providers.yfinance.client as yf_client

    monkeypatch.setattr(yf_client, "YFinanceClient", DummyYF, raising=True)
    monkeypatch.setenv("IRRELEVANT_PROFILE_TICKERS", "IBM,ORCL")
    monkeypatch.delenv("SHARADAR_API_KEY", raising=False)
    monkeypatch.setattr(cli_mod, "_atomic_write_json", lambda path, payload: None, raising=True)

    runner = CliRunner()
    res = runner.invoke(
        cli_mod.main,
        [
            "tune",
            "profile",
            "--kind",
            "financials",
            "--output-dir",
            str(tmp_path),
            "--tickers",
            "CUSTOM1,CUSTOM2",
        ],
    )

    assert res.exit_code == 0
    assert captured["tickers"] == ["CUSTOM1", "CUSTOM2"]


def test_tune_profile_rejects_empty_ticker_tokens(tmp_path: Path) -> None:
    runner = CliRunner()

    res = runner.invoke(
        cli_mod.main,
        ["tune", "profile", "--kind", "financials", "--output-dir", str(tmp_path), "--tickers", "AAPL,"],
    )

    assert res.exit_code != 0
    assert "tickers must be a comma-separated list of non-empty symbols" in res.output


def test_tune_sweep_sampling_and_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _run_sweep_measurements(combos, out_dir, include_sharadar):  # type: ignore[no-untyped-def]
        return {"results": [], "best": {"client_config": {"concurrency": 8}}}

    def _score_and_rank_results(results, rank_by, rank_alpha, rank_error_penalty):  # type: ignore[no-untyped-def]
        return results

    monkeypatch.setattr(cli_mod, "_run_sweep_measurements", _run_sweep_measurements, raising=True)
    monkeypatch.setattr(cli_mod, "_score_and_rank_results", _score_and_rank_results, raising=True)
    captured = {}

    def _capture(path: Path, payload: object) -> None:
        captured.setdefault("paths", []).append(path.name)
        captured["payload"] = payload

    monkeypatch.setattr(cli_mod, "_atomic_write_json", _capture, raising=True)
    out = tmp_path / "sweep"
    runner = CliRunner()
    res = runner.invoke(
        cli_mod.main,
        [
            "tune",
            "sweep",
            "--output-dir",
            str(out),
            "--concurrency-list",
            "8,12,16",
            "--sample-count",
            "2",
        ],
    )
    assert res.exit_code == 0
    assert "profile_sweep_results.json" in res.output
    assert "profile_tuning_best.json" in ",".join(captured.get("paths", []))


def test_tune_export_best_writes_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out"
    out.mkdir()
    # Monkeypatch Path.exists and read_text for best file
    original_exists = cli_mod.Path.exists
    original_read_text = cli_mod.Path.read_text

    def _exists(self: Path) -> bool:  # type: ignore[no-redef]
        return self.name == "profile_tuning_best.json" or original_exists(self)

    def _read_text(self: Path) -> str:  # type: ignore[no-redef]
        if self.name == "profile_tuning_best.json":
            return (
                "{"
                '"yfinance_price":{"client_config":{"concurrency":8}},'
                '"yfinance_financials":{"client_config":{"http_timeout_s":30}}'
                "}"
            )
        return original_read_text(self)

    monkeypatch.setattr(cli_mod.Path, "exists", _exists, raising=False)
    monkeypatch.setattr(cli_mod.Path, "read_text", _read_text, raising=False)
    captured = {}

    def _write_text(path: Path, content: str) -> None:
        captured["path"] = path
        captured["content"] = content

    monkeypatch.setattr(cli_mod, "_atomic_write_text", _write_text, raising=True)
    runner = CliRunner()
    write_file = out / "exports.sh"
    res = runner.invoke(
        cli_mod.main,
        ["tune", "export-best", "--output-dir", str(out), "--write-file", str(write_file)],
    )
    assert res.exit_code == 0
    assert captured.get("path") == write_file
    assert '"concurrency": 8' in captured.get("content", "")


def test_tune_export_best_missing_file(tmp_path: Path) -> None:
    out = tmp_path / "no_best"
    out.mkdir()
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["tune", "export-best", "--output-dir", str(out)])
    assert res.exit_code != 0
    assert "Best file not found" in res.output


def test_tune_export_best_prints_stdout(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    best = out / "profile_tuning_best.json"
    best.write_text(
        "{"
        '"yfinance_price":{"client_config":{"concurrency":8}},'
        '"yfinance_financials":{"client_config":{"http_timeout_s":30}}'
        "}"
    )
    runner = CliRunner()
    res = runner.invoke(cli_mod.main, ["tune", "export-best", "--output-dir", str(out)])
    assert res.exit_code == 0
    assert "create_client(..., **yfinance_price)" in res.output
