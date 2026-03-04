from vertex_forager.cli import _build_sweep_combinations, _score_and_rank_results

def test_build_sweep_combinations():
    # Test with default sampling (50)
    combos = _build_sweep_combinations(
        concurrency_list=None,
        flush_rows_list=None,
        keepalive_list=None,
        connections_list=None,
        timeout_list=None,
        sample_count=None,
        sample_seed=42,
    )
    # 5 * 5 * 4 * 4 * 2 = 800 combinations total, capped at 50 default
    assert len(combos) == 50

    # Test with custom lists
    combos = _build_sweep_combinations(
        concurrency_list="10,20",
        flush_rows_list="100",
        keepalive_list="50",
        connections_list="100",
        timeout_list="30",
        sample_count=None,
        sample_seed=42,
    )
    # 2 * 1 * 1 * 1 * 1 = 2 combos
    assert len(combos) == 2
    assert combos[0]["VF_CONCURRENCY"] in [10, 20]
    assert combos[0]["VF_FLUSH_THRESHOLD_ROWS"] == 100

def test_score_and_rank_results():
    mock_results = {
        "runs": [
            {
                "env": {"VF_CONCURRENCY": 10},
                "measurements": {
                    "yfinance_price": {"duration_s": 10.0, "metrics": {"errors": []}},
                    "yfinance_financials": {"duration_s": 5.0, "metrics": {"errors": []}},
                }
            },
            {
                "env": {"VF_CONCURRENCY": 20},
                "measurements": {
                    "yfinance_price": {"duration_s": 20.0, "metrics": {"errors": []}},
                    "yfinance_financials": {"duration_s": 8.0, "metrics": {"errors": []}},
                }
            },
            {
                "env": {"VF_CONCURRENCY": 30},
                "measurements": {
                    "yfinance_price": {"duration_s": 5.0, "metrics": {"errors": ["e1"]}}, # Error penalty
                    "yfinance_financials": {"duration_s": 5.0, "metrics": {"errors": []}},
                }
            }
        ]
    }

    # Rank by duration (lower is better)
    ranked = _score_and_rank_results(
        mock_results,
        rank_by="duration",
        rank_alpha=0.0,
        rank_error_penalty=100.0
    )
    
    # Best Price Run
    # Run 1: 10.0s
    # Run 2: 20.0s
    # Run 3: 5.0s + 100.0 (penalty) = 105.0s
    # Winner: Run 1
    best_price = ranked["best"]["yfinance_price"]
    assert best_price["env"]["VF_CONCURRENCY"] == 10

    # Best Financials Run
    # Run 1: 5.0s
    # Run 2: 8.0s
    # Run 3: 5.0s
    # Tie-break (stable sort): Run 1 comes first
    best_fin = ranked["best"]["yfinance_financials"]
    assert best_fin["measurements"]["yfinance_financials"]["duration_s"] == 5.0
