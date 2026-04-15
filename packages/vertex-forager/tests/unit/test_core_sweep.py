from __future__ import annotations

import logging

import pytest

from vertex_forager.core.sweep import _best_for_key, _score_run, build_sweep_combinations, score_and_rank_results


def test_build_sweep_combinations_runs_all_products_when_sample_not_restrictive() -> None:
    combos, message = build_sweep_combinations(
        concurrency_list="1,2",
        flush_rows_list="10",
        keepalive_list="3",
        connections_list="4,5",
        timeout_list="6",
        sample_count=None,
        sample_seed=7,
    )

    assert len(combos) == 4
    assert message == "Running all 4 combinations."
    assert combos[0]["concurrency"] == 1
    assert combos[-1]["limits"]["max_connections"] == 5


def test_build_sweep_combinations_sampling_is_deterministic() -> None:
    kwargs = {
        "concurrency_list": "1,2,3",
        "flush_rows_list": "10,20",
        "keepalive_list": "4,5",
        "connections_list": "6,7",
        "timeout_list": "8",
        "sample_count": 3,
        "sample_seed": 123,
    }

    first, first_message = build_sweep_combinations(**kwargs)
    second, second_message = build_sweep_combinations(**kwargs)

    assert first == second
    assert first_message == second_message
    assert len(first) == 3
    assert "Sampling 3 combinations from 24 total" in first_message


@pytest.mark.parametrize(
    ("field", "value", "pattern"),
    [
        ("concurrency_list", "1,,2", "Invalid empty value"),
        ("flush_rows_list", "-1", "must be positive"),
    ],
)
def test_build_sweep_combinations_rejects_invalid_numeric_lists(
    field: str,
    value: str,
    pattern: str,
) -> None:
    kwargs = {
        "concurrency_list": None,
        "flush_rows_list": None,
        "keepalive_list": None,
        "connections_list": None,
        "timeout_list": None,
        "sample_count": None,
        "sample_seed": 1,
    }
    kwargs[field] = value

    with pytest.raises(ValueError, match=pattern):
        build_sweep_combinations(**kwargs)


def test_score_run_uses_p95_and_error_penalty() -> None:
    logger = logging.getLogger("test")
    run = {
        "measurements": {
            "yfinance_price": {
                "duration_s": 2.0,
                "metrics": {
                    "summary": {"http_duration_s_p95": 4.0},
                    "errors": ["boom", "bang"],
                },
            }
        }
    }

    score = _score_run(
        run=run,
        run_key="yfinance_price",
        rank_by="duration_p95",
        rank_alpha=0.5,
        rank_error_penalty=3.0,
        logger=logger,
    )

    assert score == 10.0


def test_best_for_key_skips_error_entries_and_score_and_rank_populates_best() -> None:
    logger = logging.getLogger("test")
    results = {
        "runs": [
            {
                "id": "bad",
                "measurements": {
                    "yfinance_price": {"error": "failed"},
                    "yfinance_financials": {"error": "failed"},
                },
            },
            {
                "id": "slow",
                "measurements": {
                    "yfinance_price": {
                        "duration_s": 3.0,
                        "metrics": {"summary": {"http_duration_s_p95": 6.0}, "errors": []},
                    },
                    "yfinance_financials": {"error": "failed"},
                },
            },
            {
                "id": "best",
                "measurements": {
                    "yfinance_price": {
                        "duration_s": 1.0,
                        "metrics": {"summary": {"http_duration_s_p95": 2.0}, "errors": []},
                    },
                    "yfinance_financials": {"error": "failed"},
                },
            },
        ]
    }

    best = _best_for_key(
        results=results,
        run_key="yfinance_price",
        rank_by="duration_p95",
        rank_alpha=0.25,
        rank_error_penalty=5.0,
        logger=logger,
    )

    ranked = score_and_rank_results(
        results=results,
        rank_by="duration_p95",
        rank_alpha=0.25,
        rank_error_penalty=5.0,
        logger=logger,
    )

    assert best["id"] == "best"
    assert ranked["best"]["yfinance_price"]["id"] == "best"
    assert ranked["best"]["yfinance_financials"] == {}
