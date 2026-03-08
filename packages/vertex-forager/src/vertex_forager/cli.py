import click
import httpx
import logging
from typing import Any, Optional, cast
from pathlib import Path
import os
import json
import time
from .utils import get_app_root, get_cache_dir, clear_app_cache, cleanup_dlq_tmp
import itertools
import random
import asyncio
from datetime import datetime
import polars as pl
from vertex_forager.core.config import FramePacket
from vertex_forager.writers.duckdb import DuckDBWriter

logger = logging.getLogger(__name__)


@click.group()
def main() -> None:
    """Vertex Forager: High-performance Financial Data Collector.

    Main entry point for the CLI application.
    Manages subcommands for data collection and system status.

    Args:
        None

    Returns:
        None: Entry point does not return a value.
    """
    # Executed via `uv run`; no additional runtime checks required here.
    pass


@main.command()
@click.option("--symbol", "-s", multiple=True, help="Symbols to collect (e.g., AAPL)")
@click.option(
    "--source", type=click.Choice(["yfinance", "sharadar"]), default="sharadar"
)
def collect(symbol: tuple[str, ...], source: str) -> None:
    """Collect market data from the selected provider.

    Args:
        symbol: Tuple of ticker symbols to collect (e.g., ('AAPL', 'MSFT')).
        source: Data source ('yfinance' or 'sharadar').

    Returns:
        None: Results are printed to stdout or written to storage.

    Raises:
        ValueError: Missing API key or invalid inputs.
        httpx.RequestError: Network request fails.
    """
    if not symbol:
        raise click.UsageError("Please provide at least one symbol via --symbol.")

    click.echo(f"🚀 Starting collection for {symbol} using {source}...")

    try:
        from vertex_forager.clients import create_client
        import asyncio
        import os
        import polars as pl
        from collections.abc import Mapping

        # API Key lookup
        api_key = None
        if source == "sharadar":
            api_key_env = "SHARADAR_API_KEY"
            api_key = os.getenv(api_key_env)
            if not api_key:
                raise click.ClickException(f"Environment variable {api_key_env} is not set.")

        async def _run_collect() -> Optional[Any]:
            async with create_client(
                provider=source, api_key=api_key
            ) as client:
                if source == "sharadar":
                    from vertex_forager.providers.sharadar.client import SharadarClient
                    # For Sharadar, we use the specialized client method
                    # In the future, this can be generalized via a CollectorCore interface
                    sc = cast(SharadarClient, client)
                    result = await sc.get_price_data(tickers=list(symbol))
                    return result
                else:
                    raise click.ClickException(f"`{source}` is not supported by `collect` yet.")

        result = asyncio.run(_run_collect())

        if result is not None:
            if hasattr(result, "tables") and isinstance(result.tables, Mapping):
                total_rows = sum(result.tables.values())
                click.echo(f"✅ Completed: processed {total_rows} rows.")
                for table, count in result.tables.items():
                    click.echo(f"  - {table}: {count} rows")
            elif isinstance(result, pl.DataFrame):
                click.echo(f"✅ Completed: processed {len(result)} rows.")
            else:
                click.echo(f"✅ Completed: {result}")

    except click.ClickException:
        raise
    except (ValueError, KeyError) as e:
        logger.error("Collection failed: %s", e)
        raise click.ClickException(f"Collection error: {e}")
    except httpx.RequestError as e:
        logger.warning("Collection request failed: %s", type(e).__name__)
        raise click.ClickException("Collection error: network request failed.")
    except Exception:
        click.echo("❌ Unexpected error during collection.")
        logger.exception("Unexpected error during collection")
        raise


@main.command()
def status() -> None:
    """Check current data storage and system status.

    Args:
        None

    Returns:
        None: Status information is printed to stdout.
    """
    root: Path = get_app_root()
    click.echo(f"📂 Data Root: {root}")
    click.echo(f"📦 Cache Dir: {get_cache_dir()}")

    size = 0
    for f in root.glob("**/*"):
        try:
            if f.is_file():
                size += f.stat().st_size
        except (PermissionError, OSError) as exc:
            logger.warning("Failed to stat file during size calculation: %s", exc)
            continue

    click.echo(f"📊 Total Data Size: {size / (1024 * 1024):.2f} MB")


@main.command()
@click.option(
    "--section",
    type=click.Choice(["global", "yfinance", "sharadar", "writers", "flow", "queue", "all"]),
    default="all",
    help="Select constants section to preview",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "table"]),
    default="json",
    help="Output format (json/table)",
)
@click.option(
    "--env-only",
    is_flag=True,
    default=False,
    help="Show only keys overridden by environment variables (table format only)",
)
def constants(section: str, output_format: str, env_only: bool) -> None:
    """Preview centralized constants by section.

    Args:
        section: Constants section to render
        output_format: Output format (json/table)
        env_only: Filter table to show only environment-overridden keys
    """
    import json
    import os
    from vertex_forager import constants as global_constants
    from vertex_forager.providers.yfinance import constants as yf_constants
    from vertex_forager.providers.sharadar import constants as sh_constants
    
    preview: dict[str, dict[str, object]] = {}
    
    def add(name: str, values: dict[str, object]) -> None:
        preview[name] = values
    
    def _global_constants() -> dict[str, object]:
        return {
            "HTTP_TIMEOUT_S": global_constants.HTTP_TIMEOUT_S,
            "HTTP_MAX_CONNECTIONS": global_constants.HTTP_MAX_CONNECTIONS,
            "HTTP_MAX_KEEPALIVE_CONNECTIONS": global_constants.HTTP_MAX_KEEPALIVE_CONNECTIONS,
            "DEFAULT_RATE_LIMIT": global_constants.DEFAULT_RATE_LIMIT,
            "DEFAULT_RETRY_MAX_ATTEMPTS": global_constants.DEFAULT_RETRY_MAX_ATTEMPTS,
            "DEFAULT_RETRY_BASE_BACKOFF_S": global_constants.DEFAULT_RETRY_BASE_BACKOFF_S,
            "DEFAULT_RETRY_MAX_BACKOFF_S": global_constants.DEFAULT_RETRY_MAX_BACKOFF_S,
            "FLUSH_THRESHOLD_ROWS": global_constants.FLUSH_THRESHOLD_ROWS,
            "PRIORITY_PAGINATION": global_constants.PRIORITY_PAGINATION,
            "PRIORITY_NEW_JOB": global_constants.PRIORITY_NEW_JOB,
            "PRIORITY_SENTINEL": global_constants.PRIORITY_SENTINEL,
            "PROGRESS_LOG_CHUNK_ROWS": global_constants.PROGRESS_LOG_CHUNK_ROWS,
            "DEFAULT_TIME_ZONE": global_constants.DEFAULT_TIME_ZONE,
        }
    
    def _flow_constants() -> dict[str, object]:
        return {
            "DEFAULT_AVG_LATENCY_S": global_constants.DEFAULT_AVG_LATENCY_S,
            "CONCURRENCY_MIN": global_constants.CONCURRENCY_MIN,
            "CONCURRENCY_MAX": global_constants.CONCURRENCY_MAX,
            "GRADIENT_QUEUE_SIZE_DEFAULT": global_constants.GRADIENT_QUEUE_SIZE_DEFAULT,
            "GRADIENT_SMOOTHING_DEFAULT": global_constants.GRADIENT_SMOOTHING_DEFAULT,
            "GRADIENT_WINDOW_S": global_constants.GRADIENT_WINDOW_S,
        }
    
    def _queue_constants() -> dict[str, object]:
        return {
            "QUEUE_TARGET_RAM_RATIO": global_constants.QUEUE_TARGET_RAM_RATIO,
            "PACKET_SIZE_EST_BYTES": global_constants.PACKET_SIZE_EST_BYTES,
            "QUEUE_MIN": global_constants.QUEUE_MIN,
            "QUEUE_MAX": global_constants.QUEUE_MAX,
            "QUEUE_DEFAULT": global_constants.QUEUE_DEFAULT,
        }
    
    if section in ("global", "all"):
        add("global", _global_constants())
    if section in ("flow", "all"):
        add("flow", _flow_constants())
    if section in ("queue", "all"):
        add("queue", _queue_constants())
    
    if section in ("yfinance", "all"):
        add(
            "yfinance",
            {
                "PRICE_BATCH_SIZE": yf_constants.PRICE_BATCH_SIZE,
                "PRICE_BATCH_MAX": yf_constants.PRICE_BATCH_MAX,
                "THREADS_THRESHOLD": yf_constants.THREADS_THRESHOLD,
                "PRICE_BATCH_SIZE_KEY": yf_constants.PRICE_BATCH_SIZE_KEY,
                "DEFAULT_INTERVAL": yf_constants.DEFAULT_INTERVAL,
                "DEFAULT_PRICE_PERIOD": yf_constants.DEFAULT_PRICE_PERIOD,
            },
        )
    
    if section in ("sharadar", "all"):
        add(
            "sharadar",
            {
                "MAX_ROWS_PER_REQUEST": sh_constants.MAX_ROWS_PER_REQUEST,
                "DEFAULT_BATCH_SIZE": sh_constants.DEFAULT_BATCH_SIZE,
                "MIN_BATCH_SIZE": sh_constants.MIN_BATCH_SIZE,
                "TRADING_DAYS_RATIO": sh_constants.TRADING_DAYS_RATIO,
                "QUARTERLY_DAYS_RATIO": sh_constants.QUARTERLY_DAYS_RATIO,
                "PAGINATION_META_KEY": sh_constants.PAGINATION_META_KEY,
                "PAGINATION_CURSOR_PARAM": sh_constants.PAGINATION_CURSOR_PARAM,
                "MAX_PAGES": sh_constants.MAX_PAGES,
            },
        )
    
    if section in ("writers", "all"):
        add(
            "writers",
            {
                "WRITER_DUCKDB_MAX_WORKERS": global_constants.WRITER_DUCKDB_MAX_WORKERS,
                "WAL_AUTOCHECKPOINT_LIMIT": global_constants.WAL_AUTOCHECKPOINT_LIMIT,
            },
        )
    
    ENV_VAR_MAPPING: dict[str, tuple[str, str]] = {
        "VF_HTTP_TIMEOUT_S": ("global", "HTTP_TIMEOUT_S"),
        "VF_HTTP_MAX_CONNECTIONS": ("global", "HTTP_MAX_CONNECTIONS"),
        "VF_HTTP_MAX_KEEPALIVE": ("global", "HTTP_MAX_KEEPALIVE_CONNECTIONS"),
        "VF_FLUSH_THRESHOLD_ROWS": ("global", "FLUSH_THRESHOLD_ROWS"),
        "VF_CONCURRENCY": ("flow", "CONCURRENCY_MAX"),
    }
    env_vals = {
        "SHARADAR_API_KEY": os.getenv("SHARADAR_API_KEY"),
        **{k: os.getenv(k) for k in ENV_VAR_MAPPING.keys()},
        "VF_METRICS_ENABLED": os.getenv("VF_METRICS_ENABLED"),
        "VF_MEM_THRESHOLD_RATIO": os.getenv("VF_MEM_THRESHOLD_RATIO"),
        "VF_MEM_THRESHOLD_ABS_MB": os.getenv("VF_MEM_THRESHOLD_ABS_MB"),
    }
    env_overrides_obj: dict[str, object] = {k: v for k, v in env_vals.items() if v is not None}
    if "SHARADAR_API_KEY" in env_overrides_obj:
        env_overrides_obj["SHARADAR_API_KEY"] = "<redacted>"
    if env_overrides_obj:
        preview["env_overrides"] = env_overrides_obj
    
    if output_format == "json":
        click.echo(json.dumps(preview, indent=2, ensure_ascii=False))
        return
    
    # table format
    override_flags: dict[str, set[str]] = {
        "global": set(),
        "yfinance": set(),
        "sharadar": set(),
        "writers": set(),
        "flow": set(),
        "queue": set(),
    }
    if "env_overrides" in preview and isinstance(preview["env_overrides"], dict):
        envs = preview["env_overrides"]
        for env_name, (section_name, const_name) in ENV_VAR_MAPPING.items():
            if env_name in envs:
                override_flags[section_name].add(const_name)
    for name, values in preview.items():
        click.echo(f"\n[{name}]")
        keys = list(values.keys())
        if env_only and name in override_flags:
            keys = [k for k in keys if k in override_flags[name]]
        max_key = max((len(k) for k in keys), default=0)
        for k in keys:
            v = values[k]
            suffix = ""
            if name in override_flags and k in override_flags[name]:
                suffix = " (env)"
            click.echo(f"{k.ljust(max_key)}{suffix}  :  {v}")

@main.command()
def clear() -> None:
    """Clear all temporary cache data.

    Args:
        None

    Returns:
        None: Confirmation message is printed to stdout.
    """
    if click.confirm("⚠️ Delete all cache data?"):
        clear_app_cache()
        click.echo("🧹 Cache cleared.")

@main.group()
def tune() -> None:
    """Tuning and profiling commands.

    Args:
        None

    Returns:
        None
    """
    pass

@tune.command("profile")
@click.option("--kind", type=click.Choice(["price","financials"]), default="price")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--tickers", type=str, default=None)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
def tune_profile(kind: str, output_dir: Path | None, tickers: str | None, start_date: str | None, end_date: str | None) -> None:
    """Run a single profiling run for price or financials data.

    Args:
        kind: Type of profile ('price' or 'financials').
        output_dir: Directory for result JSON. If None, uses VF_PROFILE_OUTPUT_DIR or default path.
        tickers: Comma-separated list of ticker symbols (e.g., 'AAPL,MSFT'). If None, uses defaults.
        start_date: Start date for price data (YYYY-MM-DD). If None, uses provider default.
        end_date: End date for price data (YYYY-MM-DD). If None, uses provider default.

    Returns:
        None: Results are saved to a JSON file and the path is printed to stdout.
    """
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    out_dir.mkdir(parents=True, exist_ok=True)
    if kind == "price":
        from vertex_forager.providers.yfinance.client import YFinanceClient
        from vertex_forager.utils import as_dict
        db_path = out_dir / "profile_run.duckdb"
        if db_path.exists():
            db_path.unlink()
        os.environ.setdefault("VF_METRICS_ENABLED", "1")
        client = YFinanceClient(rate_limit=60, metrics_enabled=True, structured_logs=False)
        _tickers = [t.strip().upper() for t in (tickers or "AAPL,MSFT,NVDA,GOOGL,AMZN").split(",")]
        run = asyncio.run(client.get_price_data(tickers=_tickers, connect_db=db_path, show_progress=False, start_date=start_date, end_date=end_date))
        data = as_dict(run)
        metrics_path = out_dir / "profile_metrics.json"
        metrics_path.write_text(json.dumps(data, indent=2))
        click.echo(str(metrics_path))
        if db_path.exists():
            db_path.unlink()
    else:
        from vertex_forager.providers.yfinance.client import YFinanceClient
        from vertex_forager.providers.sharadar.client import SharadarClient
        from vertex_forager.utils import as_dict
        db_path = out_dir / "profile_financials.duckdb"
        if db_path.exists():
            db_path.unlink()
        os.environ.setdefault("VF_METRICS_ENABLED", "1")
        yf_tickers = [t.strip().upper() for t in (tickers or os.getenv("YF_TICKERS") or "AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,NFLX,ADBE,CSCO").split(",")]
        yfc = YFinanceClient(rate_limit=60, structured_logs=False)
        yf_run = asyncio.run(yfc.get_financials(kind="income_stmt", period="annual", tickers=yf_tickers, connect_db=db_path, show_progress=False))
        sh_key = os.getenv("SHARADAR_API_KEY")
        sh_run = None
        if sh_key:
            try:
                shc = SharadarClient(api_key=sh_key, rate_limit=60, structured_logs=False)
                sh_run = asyncio.run(shc.get_fundamental_data(tickers=yf_tickers[:5], connect_db=db_path, dimension="MRT"))
            except Exception as e:
                logger.warning(f"Sharadar verification skipped due to error: {e}")
                sh_run = None
        data = {
            "yfinance_financials": as_dict(yf_run),
            "sharadar_sf1_optional": as_dict(sh_run),
        }

        metrics_path = out_dir / "profile_financials_metrics.json"
        metrics_path.write_text(json.dumps(data, indent=2))
        click.echo(str(metrics_path))
        if db_path.exists():
            db_path.unlink()

def _build_sweep_combinations(
    concurrency_list: str | None,
    flush_rows_list: str | None,
    keepalive_list: str | None,
    connections_list: str | None,
    timeout_list: str | None,
    sample_count: int | None,
    sample_seed: int,
) -> list[dict[str, Any]]:
    """Parse list arguments and generate sweep combinations.

    Args:
        concurrency_list: Comma-separated concurrency values.
        flush_rows_list: Comma-separated flush threshold values.
        keepalive_list: Comma-separated keepalive values.
        connections_list: Comma-separated connection values.
        timeout_list: Comma-separated timeout values.
        sample_count: Number of combinations to sample (optional).
        sample_seed: Seed for random sampling.

    Returns:
        List of environment configuration dictionaries.
    """
    def _parse_list(s: str | None, default: list[int]) -> list[int]:
        if not s:
            return default
        vals: list[int] = []
        for tok in s.split(","):
            tok = tok.strip()
            if not tok:
                raise click.BadParameter(f"Invalid empty value in list '{s}'.")
            try:
                x = int(tok)
                if x <= 0:
                    raise ValueError("Must be positive")
                vals.append(x)
            except ValueError:
                raise click.BadParameter(f"Invalid value '{tok}' in list '{s}'. Must be positive integers.")
        return vals

    concs = _parse_list(concurrency_list, [8, 12, 16, 20, 24])
    flushes = _parse_list(flush_rows_list, [100_000, 150_000, 200_000, 250_000, 300_000])
    keepalives = _parse_list(keepalive_list, [100, 150, 200, 250])
    connections = _parse_list(connections_list, [200, 300, 400, 500])
    timeouts = _parse_list(timeout_list, [30, 45])

    combos: list[dict[str, Any]] = []
    for c, f, k, m, t in itertools.product(concs, flushes, keepalives, connections, timeouts):
        combos.append({
            "VF_CONCURRENCY": c,
            "VF_FLUSH_THRESHOLD_ROWS": f,
            "VF_HTTP_MAX_KEEPALIVE": k,
            "VF_HTTP_MAX_CONNECTIONS": m,
            "VF_HTTP_TIMEOUT_S": t,
        })
    
    total = len(combos)
    if sample_count is None:
        sample_count = 50  # Default sane limit

    if sample_count > 0 and sample_count < total:
        click.echo(f"Sampling {sample_count} combinations from {total} total (seed={sample_seed}).")
        rnd = random.Random(sample_seed)
        combos = rnd.sample(combos, sample_count)
    else:
        click.echo(f"Running all {total} combinations.")
        
    return combos

def _run_sweep_measurements(
    combos: list[dict[str, Any]],
    output_dir: Path,
    include_sharadar: bool,
) -> dict[str, Any]:
    """Execute sweep measurements for each configuration.

    Args:
        combos: List of configuration dictionaries.
        output_dir: Directory for temporary database files.
        include_sharadar: Whether to include Sharadar benchmarks.

    Returns:
        Dictionary containing run results.
    """
    from vertex_forager.providers.yfinance.client import YFinanceClient
    from vertex_forager.providers.sharadar.client import SharadarClient
    from vertex_forager.utils import set_env, load_tickers_env
    
    # Use output_dir for DB paths
    out_dir = output_dir

    yf_tickers_price = load_tickers_env("YF_TICKERS_PRICE", ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"])
    yf_tickers_fin = load_tickers_env("YF_TICKERS_FIN", ["AAPL", "MSFT", "NVDA"])
    yf_start = os.getenv("YF_PRICE_START_DATE")
    yf_end = os.getenv("YF_PRICE_END_DATE")
    sh_key = os.getenv("SHARADAR_API_KEY") if include_sharadar else None
    sh_tickers = load_tickers_env("SH_TICKERS", ["AAPL", "MSFT", "NVDA"])
    sh_start = os.getenv("SH_START_DATE")
    sh_end = os.getenv("SH_END_DATE")
    results: dict[str, Any] = {"runs": []}
    original_env = os.environ.copy()
    try:
        for idx, cfg in enumerate(combos):
            combo_db_path = out_dir / f"profile_sweep_{idx}.duckdb"
            if combo_db_path.exists():
                combo_db_path.unlink()
            
            try:
                set_env(cfg)
                entry: dict[str, Any] = {"env": dict(cfg), "measurements": {}}
                
                # YFinance Price
                try:
                    yfc = YFinanceClient(rate_limit=60, structured_logs=False)
                    t0 = time.monotonic()
                    yf_price = asyncio.run(yfc.get_price_data(tickers=yf_tickers_price, connect_db=combo_db_path, show_progress=False, start_date=yf_start, end_date=yf_end))
                    t1 = time.monotonic()
                    entry["measurements"]["yfinance_price"] = {
                        "duration_s": round(t1 - t0, 3),
                        "metrics": {
                            "summary": getattr(yf_price, "metrics_summary", {}),
                            "counters": getattr(yf_price, "metrics_counters", {}),
                            "errors": getattr(yf_price, "errors", []),
                        },
                    }
                except Exception as e:
                    logger.warning(f"YFinance Price sweep error: {e}")
                    entry["measurements"]["yfinance_price"] = {"error": str(e)}

                # YFinance Financials
                try:
                    yfc2 = YFinanceClient(rate_limit=60, structured_logs=False)
                    t2 = time.monotonic()
                    yf_fin = asyncio.run(yfc2.get_financials(kind="income_stmt", period="annual", tickers=yf_tickers_fin, connect_db=combo_db_path, show_progress=False))
                    t3 = time.monotonic()
                    entry["measurements"]["yfinance_financials"] = {
                        "duration_s": round(t3 - t2, 3),
                        "metrics": {
                            "summary": getattr(yf_fin, "metrics_summary", {}),
                            "counters": getattr(yf_fin, "metrics_counters", {}),
                            "errors": getattr(yf_fin, "errors", []),
                        },
                    }
                except Exception as e:
                    logger.warning(f"YFinance Financials sweep error: {e}")
                    entry["measurements"]["yfinance_financials"] = {"error": str(e)}

                # Sharadar
                if sh_key:
                    try:
                        shc = SharadarClient(api_key=sh_key, rate_limit=60, structured_logs=False)
                        t4 = time.monotonic()
                        sh_fin = asyncio.run(shc.get_fundamental_data(tickers=sh_tickers, connect_db=combo_db_path, dimension="MRT", start_date=sh_start, end_date=sh_end))
                        t5 = time.monotonic()
                        entry["measurements"]["sharadar_sf1_mrt"] = {
                            "duration_s": round(t5 - t4, 3),
                            "metrics": {
                                "summary": getattr(sh_fin, "metrics_summary", {}),
                                "counters": getattr(sh_fin, "metrics_counters", {}),
                                "errors": getattr(sh_fin, "errors", []),
                            },
                        }
                    except Exception as e:
                        logger.warning(f"Sharadar sweep error: {e}")
                        entry["measurements"]["sharadar_sf1_mrt"] = {"error": str(e)}
                
                results["runs"].append(entry)
            
            finally:
                # Cleanup per-combo DB
                if combo_db_path.exists():
                    try:
                        combo_db_path.unlink()
                    except OSError as e:
                        logger.warning(f"Failed to cleanup temp DB {combo_db_path}: {e}")

                # Restore environment safely
                current_env = dict(os.environ)
                added_keys = current_env.keys() - original_env.keys()
                for k in added_keys:
                    os.environ.pop(k, None)
                for k, v in original_env.items():
                    os.environ[k] = v
                
                # Re-apply metrics enabled as it's required for all runs
                os.environ.setdefault("VF_METRICS_ENABLED", "1")

    finally:
        # Final environment restore
        current_env = dict(os.environ)
        added_keys = current_env.keys() - original_env.keys()
        for k in added_keys:
            os.environ.pop(k, None)
        for k, v in original_env.items():
            os.environ[k] = v
            
    return results

def _score_and_rank_results(
    results: dict[str, Any],
    rank_by: str,
    rank_alpha: float,
    rank_error_penalty: float,
) -> dict[str, Any]:
    """Score and rank the sweep results.

    Args:
        results: Dictionary containing 'runs' list.
        rank_by: Ranking metric ('duration' or 'duration_p95').
        rank_alpha: Weight for p95 duration in scoring.
        rank_error_penalty: Penalty multiplier for error count.

    Returns:
        Updated results dictionary with 'best' entry added.
    """
    def _score(r: dict[str, Any], run_key: str) -> float:
        m = r["measurements"].get(run_key, {})
        duration = m.get("duration_s", float("inf"))
        metrics = m.get("metrics", {})
        errors = metrics.get("errors", [])
        err_cnt = len(errors) if isinstance(errors, list) else 0
        
        try:
            score = float(duration)
        except (ValueError, TypeError):
            score = float("inf")
        
        if rank_by == "duration_p95":
            summary = metrics.get("summary", {})
            p95 = summary.get("http_duration_s_p95", duration)
            try:
                score += float(rank_alpha) * float(p95)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error computing p95 score: {e}, inputs: alpha={rank_alpha}, p95={p95}")
        
        if err_cnt > 0:
            try:
                score += float(err_cnt) * float(rank_error_penalty)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error computing penalty score: {e}, inputs: err_cnt={err_cnt}, penalty={rank_error_penalty}")
                score += float(err_cnt) * 5.0 # Fallback penalty

        return score

    def _best(run_key: str) -> dict[str, Any]:
        runs = cast(list[dict[str, Any]], results.get("runs", []))
        if not runs:
            return {}
        
        # Filter runs that actually have measurements for the given key and no explicit error
        valid_runs = []
        for r in runs:
            m = r.get("measurements", {}).get(run_key)
            if m and not m.get("error"):
                valid_runs.append(r)
        
        if not valid_runs:
            return {}

        ranked: list[dict[str, Any]] = sorted(valid_runs, key=lambda r: _score(r, run_key))
        return ranked[0]
        
    results["best"] = {
        "yfinance_price": _best("yfinance_price"),
        "yfinance_financials": _best("yfinance_financials"),
    }
    return results

@tune.command("sweep")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--include-sharadar", is_flag=True, default=False)
@click.option("--concurrency-list", type=str, default=None, help="Comma-separated list of concurrency values")
@click.option("--flush-rows-list", type=str, default=None, help="Comma-separated list of flush threshold rows")
@click.option("--keepalive-list", type=str, default=None, help="Comma-separated list of HTTP keepalive counts")
@click.option("--connections-list", type=str, default=None, help="Comma-separated list of HTTP max connections")
@click.option("--timeout-list", type=str, default=None, help="Comma-separated list of HTTP timeouts (seconds)")
@click.option("--rank-by", type=click.Choice(["duration","duration_p95"]), default="duration")
@click.option("--rank-alpha", type=float, default=0.4)
@click.option("--sample-count", type=int, default=None)
@click.option("--sample-seed", type=int, default=42)
@click.option("--rank-error-penalty", type=float, default=5.0)
def tune_sweep(output_dir: Path | None, include_sharadar: bool, concurrency_list: str | None, flush_rows_list: str | None, keepalive_list: str | None, connections_list: str | None, timeout_list: str | None, rank_by: str, rank_alpha: float, sample_count: int | None, sample_seed: int, rank_error_penalty: float) -> None:
    """Run a parameter sweep to find optimal settings.

    Args:
        output_dir: Directory to write output files.
        include_sharadar: Whether to include Sharadar benchmarks.
        concurrency_list: List of concurrency values to test.
        flush_rows_list: List of flush thresholds to test.
        keepalive_list: List of keepalive connection limits.
        connections_list: List of total connection limits.
        timeout_list: List of timeout values.
        rank_by: Metric to rank results by.
        rank_alpha: Weight factor for p95 duration.
        sample_count: Number of random samples to run.
        sample_seed: Random seed for sampling.
        rank_error_penalty: Score penalty per error.

    Returns:
        None: Writes profile_sweep_results.json and profile_tuning_best.json.
    """
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "profile_sweep_results.json"
    best_path = out_dir / "profile_tuning_best.json"

    os.environ.setdefault("VF_METRICS_ENABLED", "1")
    
    # 1. Build combinations
    combos = _build_sweep_combinations(
        concurrency_list, flush_rows_list, keepalive_list, connections_list, timeout_list,
        sample_count, sample_seed
    )
    
    # 2. Measure
    results = _run_sweep_measurements(combos, out_dir, include_sharadar)
    
    # 3. Score and Rank
    results = _score_and_rank_results(results, rank_by, rank_alpha, rank_error_penalty)
    
    report_path.write_text(json.dumps(results, indent=2))
    best_path.write_text(json.dumps(results["best"], indent=2))
    click.echo(str(report_path))

@tune.command("export-best")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--write-file", type=click.Path(path_type=Path), default=None)
def tune_export_best(output_dir: Path | None, write_file: Path | None) -> None:
    """Export environment variables from best tuning results.

    Args:
        output_dir: Directory containing profile_tuning_best.json.
        write_file: Optional file path to write export commands to.

    Returns:
        None: Prints or writes export commands.
    """
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    best_path = out_dir / "profile_tuning_best.json"
    if not best_path.exists():
        raise click.ClickException(f"Best file not found: {best_path}")
    data = json.loads(best_path.read_text())
    lines: list[str] = []
    def _collect(label: str) -> None:
        entry = data.get(label) or {}
        env = entry.get("env") or {}
        lines.append(f"# {label} recommended exports")
        for k, v in env.items():
            lines.append(f"export {k}={v}")
        lines.append("")
    _collect("yfinance_price")
    _collect("yfinance_financials")
    content = "\n".join(lines)
    if write_file:
        write_file.write_text(content)
        click.echo(str(write_file))
    else:
        click.echo(content)

@main.command("recover")
@click.option("--dir", "dlq_dir", type=click.Path(path_type=Path), default=None, help="DLQ root directory (default: $ROOT/cache/dlq)")
@click.option("--table", "tables", multiple=True, help="Limit recovery to specific table(s)")
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=None, help="Target DuckDB file path (or set VF_RECOVER_DB)")
@click.option("--dry-run", is_flag=True, default=False, help="Scan and report without writing")
@click.option("--delete-on-success", is_flag=True, default=False, help="Delete IPC files after successful reinjection")
@click.option("--clean-tmp", is_flag=True, default=False, help="Remove stale .ipc.tmp files before recovery")
@click.option("--retention-s", type=int, default=86400, help="Retention window for .ipc.tmp cleanup (seconds)")
@click.option("--report", type=click.Path(path_type=Path), default=None, help="Write JSON report to this path")
@click.option("--progress", is_flag=True, default=False, help="Show per-file progress during recovery")
@click.option("--verbose", is_flag=True, default=False, help="Print per-file details without requiring --report")
@click.option("--strict", is_flag=True, default=False, help="Exit non-zero if any failures occur (RecoverFail/CloseFail/DeleteFail)")
def recover(dlq_dir: Path | None, tables: tuple[str, ...], db_path: Path | None, dry_run: bool, delete_on_success: bool, clean_tmp: bool, retention_s: int, report: Path | None, progress: bool, verbose: bool, strict: bool) -> None:
    """Recover failed DLQ batches into the target DuckDB database.
    
    Usage:
        vertex-forager recover --dir "$VERTEXFORAGER_ROOT/cache/dlq" --dry-run --report /tmp/dlq_report.json
        vertex-forager recover --table sharadar_sf1 --dir "$VERTEXFORAGER_ROOT/cache/dlq" --db /path/to/target.duckdb --delete-on-success
        vertex-forager recover --dir "$VERTEXFORAGER_ROOT/cache/dlq" --dry-run --strict --progress
    
    Caution:
        Deletion occurs only after a successful writer close.
        Prefer --dry-run first to preview counts and affected files.
    
    Args:
        dlq_dir: Base DLQ directory containing per-table subdirectories.
        tables: Specific table(s) to recover; if empty, recover all.
        db_path: Target DuckDB database file path (or environment VF_RECOVER_DB).
        dry_run: When True, do not write — only report counts.
        delete_on_success: Remove IPC files after successful reinjection.
        clean_tmp: Remove stale .ipc.tmp files before recovery.
        retention_s: Age threshold for cleaning .ipc.tmp files.
        report: Optional path to write a JSON summary.
        progress: Show per-file progress output.
        verbose: Print per-file details without requiring --report.
        strict: Exit non-zero if any failures occur.
    """
    try:
        base = dlq_dir or (get_cache_dir() / "dlq")
        if not base.exists():
            raise click.ClickException(f"DLQ directory not found: {base}")
        if clean_tmp:
            try:
                deleted = cleanup_dlq_tmp(base, retention_s)
                click.echo(f"🧹 Cleaned {deleted} stale .ipc.tmp files")
            except ValueError as e:
                raise click.ClickException(str(e))
        env_db = os.getenv("VF_RECOVER_DB")
        target_db = db_path or (Path(env_db) if env_db and env_db.strip() else None)
        if not dry_run and target_db is None:
            raise click.ClickException("Missing target DB. Provide --db or set VF_RECOVER_DB")
        selected_tables: list[str]
        if tables:
            raw_tables = [t.strip() for t in tables if t and t.strip()]
            seen: set[str] = set()
            selected_tables = []
            for t in raw_tables:
                if t not in seen:
                    seen.add(t)
                    selected_tables.append(t)
        else:
            # Discover table subdirs
            selected_tables = sorted([p.name for p in base.iterdir() if p.is_dir()])
        if not selected_tables:
            click.echo("No tables selected or found under DLQ.")
            return
        summary: dict[str, Any] = {"base": str(base), "db": str(target_db) if target_db else None, "dry_run": dry_run, "tables": {}, "errors": [], "error_counts": {}}
        async def _run() -> None:
            writer: DuckDBWriter | None = None
            delete_candidates: dict[str, list[Path]] = {}
            try:
                if not dry_run:
                    if target_db is None:
                        raise RuntimeError("recover: missing target DB")
                    writer = DuckDBWriter(target_db)
                for tbl in selected_tables:
                    tbl_dir = base / tbl
                    if not tbl_dir.exists():
                        continue
                    ipc_files = sorted([p for p in tbl_dir.glob("batch_*.ipc") if p.is_file()])
                    rows_scanned = 0
                    rows_written = 0
                    file_reports: list[dict[str, Any]] = []
                    for f in ipc_files:
                        try:
                            df = pl.read_ipc(f)
                            rows_scanned += int(df.height)
                            if progress:
                                click.echo(f"[scan] {tbl} {f.name} rows={int(df.height)}")
                            if dry_run:
                                file_reports.append({"file": str(f), "rows": int(df.height), "status": "scanned"})
                                continue
                            pkt = FramePacket(provider="dlq", table=tbl, frame=df, observed_at=datetime.now())
                            if writer is None:
                                raise RuntimeError("recover: writer is not initialized")
                            res = await writer.write(pkt)
                            rows_written += int(res.rows)
                            file_reports.append({"file": str(f), "rows": int(df.height), "status": "written"})
                            if progress:
                                click.echo(f"[write] {tbl} {f.name} rows={int(res.rows)}")
                            if delete_on_success:
                                delete_candidates.setdefault(tbl, []).append(f)
                        except Exception as e:
                            summary["errors"].append(f"RecoverFail:{tbl}:{f}:{e}")
                    summary["tables"][tbl] = {"files_scanned": len(ipc_files), "rows_scanned": rows_scanned, "rows_written": rows_written, "details": file_reports}
            finally:
                closed_ok = True
                if writer is not None:
                    try:
                        await writer.close()
                    except Exception as e_close:
                        closed_ok = False
                        summary["errors"].append(f"CloseFail:{e_close}")
                        logger.warning(f"Recover: writer close failed: {e_close}")
                # Perform deletions only if writer closed successfully
                if delete_on_success and not dry_run and closed_ok:
                    for tbl, files in delete_candidates.items():
                        for f in files:
                            try:
                                f.unlink()
                                try:
                                    dir_fd = os.open(str(f.parent), os.O_RDONLY)
                                    try:
                                        os.fsync(dir_fd)
                                    finally:
                                        os.close(dir_fd)
                                except Exception:
                                    pass
                                # Update status to deleted
                                details = summary["tables"].get(tbl, {}).get("details", [])
                                for entry in details:
                                    if entry.get("file") == str(f) and entry.get("status") == "written":
                                        entry["status"] = "deleted"
                            except Exception as e_del:
                                summary["errors"].append(f"DeleteFail:{tbl}:{f}:{e_del}")
                # Mark files as failed on close failure
                if delete_on_success and not dry_run and not closed_ok:
                    for tbl, files in delete_candidates.items():
                        details = summary["tables"].get(tbl, {}).get("details", [])
                        for f in files:
                            for entry in details:
                                if entry.get("file") == str(f) and entry.get("status") == "written":
                                    entry["status"] = "close_failed"
            # Aggregate error counts
            counts: dict[str, int] = {"RecoverFail": 0, "CloseFail": 0, "DeleteFail": 0}
            for msg in summary["errors"]:
                if isinstance(msg, str):
                    if msg.startswith("RecoverFail:"):
                        counts["RecoverFail"] += 1
                    elif msg.startswith("CloseFail:"):
                        counts["CloseFail"] += 1
                    elif msg.startswith("DeleteFail:"):
                        counts["DeleteFail"] += 1
            summary["error_counts"] = counts
        asyncio.run(_run())
        if report:
            try:
                Path(report).parent.mkdir(parents=True, exist_ok=True)
                Path(report).write_text(json.dumps(summary, indent=2))
                click.echo(str(report))
            except Exception as e_rep:
                raise click.ClickException(f"Failed to write report: {e_rep}")
        else:
            total_rows = sum(int(v.get("rows_written", 0)) for v in summary["tables"].values())
            ec = summary.get("error_counts", {})
            msg = f"✅ Recover summary: tables={len(summary['tables'])} rows={total_rows} errors={len(summary['errors'])}"
            if ec:
                msg += f" (RecoverFail={ec.get('RecoverFail', 0)} CloseFail={ec.get('CloseFail', 0)} DeleteFail={ec.get('DeleteFail', 0)})"
            click.echo(msg)
            if verbose:
                for tbl, info in summary["tables"].items():
                    details = info.get("details", [])
                    for d in details:
                        click.echo(f"[detail] {tbl} file={d.get('file')} rows={d.get('rows')} status={d.get('status')}")
        if strict and summary["errors"]:
            raise click.ClickException("Errors encountered during recovery. See --report for details.")
    except click.ClickException:
        raise
    except Exception as e:
        logger.exception("Unexpected recover error")
        raise click.ClickException(str(e))

if __name__ == "__main__":
    main()
