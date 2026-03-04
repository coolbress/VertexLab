import click
import httpx
import logging
from typing import Any, Optional, cast
from pathlib import Path
import os
import json
import time
from .utils import get_app_root, get_cache_dir, clear_app_cache
import itertools
import random

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
    pass

@tune.command("profile")
@click.option("--kind", type=click.Choice(["price","financials"]), default="price")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--tickers", type=str, default=None)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
def tune_profile(kind: str, output_dir: Path | None, tickers: str | None, start_date: str | None, end_date: str | None) -> None:
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    out_dir.mkdir(parents=True, exist_ok=True)
    if kind == "price":
        from vertex_forager.providers.yfinance.client import YFinanceClient
        db_path = out_dir / "profile_run.duckdb"
        if db_path.exists():
            db_path.unlink()
        client = YFinanceClient(rate_limit=60, metrics_enabled=True, structured_logs=False)
        _tickers = [t.strip().upper() for t in (tickers or "AAPL,MSFT,NVDA,GOOGL,AMZN").split(",")]
        run = client.get_price_data(tickers=_tickers, connect_db=db_path, show_progress=False, start_date=start_date, end_date=end_date)
        data = {
            "counters": getattr(run, "metrics_counters", {}),
            "histograms": getattr(run, "metrics_histograms", {}),
            "summary": getattr(run, "metrics_summary", {}),
            "tables": getattr(run, "tables", {}),
            "errors": getattr(run, "errors", []),
        }
        metrics_path = out_dir / "profile_metrics.json"
        metrics_path.write_text(json.dumps(data, indent=2))
        click.echo(str(metrics_path))
        if db_path.exists():
            db_path.unlink()
    else:
        from vertex_forager.providers.yfinance.client import YFinanceClient
        from vertex_forager.providers.sharadar.client import SharadarClient
        db_path = out_dir / "profile_financials.duckdb"
        if db_path.exists():
            db_path.unlink()
        os.environ.setdefault("VF_METRICS_ENABLED", "1")
        yf_tickers = [t.strip().upper() for t in (tickers or os.getenv("YF_TICKERS") or "AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,NFLX,ADBE,CSCO").split(",")]
        yfc = YFinanceClient(rate_limit=60, structured_logs=False)
        yf_run = yfc.get_financials(kind="income_stmt", period="annual", tickers=yf_tickers, connect_db=db_path, show_progress=False)
        sh_key = os.getenv("SHARADAR_API_KEY")
        sh_run = None
        if sh_key:
            try:
                shc = SharadarClient(api_key=sh_key, rate_limit=60, structured_logs=False)
                sh_run = shc.get_fundamental_data(tickers=yf_tickers[:5], connect_db=db_path, dimension="MRT")
            except Exception:
                sh_run = None
        def _as_dict(obj: Any) -> dict[str, Any]:
            if obj is None:
                return {}
            return {
                "counters": getattr(obj, "metrics_counters", {}),
                "histograms": getattr(obj, "metrics_histograms", {}),
                "summary": getattr(obj, "metrics_summary", {}),
                "tables": getattr(obj, "tables", {}),
                "errors": getattr(obj, "errors", []),
            }
        data = {
            "yfinance_financials": _as_dict(yf_run),
            "sharadar_sf1_optional": _as_dict(sh_run),
        }
        metrics_path = out_dir / "profile_financials_metrics.json"
        metrics_path.write_text(json.dumps(data, indent=2))
        click.echo(str(metrics_path))
        if db_path.exists():
            db_path.unlink()

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
    from vertex_forager.providers.yfinance.client import YFinanceClient
    from vertex_forager.providers.sharadar.client import SharadarClient
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "profile_sweep.duckdb"
    report_path = out_dir / "profile_sweep_results.json"
    if db_path.exists():
        db_path.unlink()
    os.environ.setdefault("VF_METRICS_ENABLED", "1")
    def _parse_list(s: str | None, default: list[int]) -> list[int]:
        if not s:
            return default
        vals: list[int] = []
        for tok in s.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                x = int(tok)
                if x > 0:
                    vals.append(x)
            except ValueError:
                continue
        return vals or default
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
    if sample_count and sample_count > 0 and sample_count < len(combos):
        rnd = random.Random(sample_seed)
        combos = rnd.sample(combos, sample_count)
    def _set_env(cfg: dict[str, Any]) -> None:
        for k, v in cfg.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
    def _load_tickers_env(name: str, default: list[str]) -> list[str]:
        v = os.getenv(name)
        if not v:
            return default
        toks = [t.strip().upper() for t in v.split(",") if t.strip()]
        return toks or default
    yf_tickers_price = _load_tickers_env("YF_TICKERS_PRICE", ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"])
    yf_tickers_fin = _load_tickers_env("YF_TICKERS_FIN", ["AAPL", "MSFT", "NVDA"])
    yf_start = os.getenv("YF_PRICE_START_DATE")
    yf_end = os.getenv("YF_PRICE_END_DATE")
    sh_key = os.getenv("SHARADAR_API_KEY") if include_sharadar else None
    sh_tickers = _load_tickers_env("SH_TICKERS", ["AAPL", "MSFT", "NVDA"])
    sh_start = os.getenv("SH_START_DATE")
    sh_end = os.getenv("SH_END_DATE")
    results: dict[str, Any] = {"runs": []}
    for cfg in combos:
        _set_env(cfg)
        entry: dict[str, Any] = {"env": dict(cfg), "measurements": {}}
        yfc = YFinanceClient(rate_limit=60, structured_logs=False)
        t0 = time.monotonic()
        yf_price = yfc.get_price_data(tickers=yf_tickers_price, connect_db=db_path, show_progress=False, start_date=yf_start, end_date=yf_end)
        t1 = time.monotonic()
        entry["measurements"]["yfinance_price"] = {
            "duration_s": round(t1 - t0, 3),
            "metrics": {
                "summary": getattr(yf_price, "metrics_summary", {}),
                "counters": getattr(yf_price, "metrics_counters", {}),
                "errors": getattr(yf_price, "errors", []),
            },
        }
        yfc2 = YFinanceClient(rate_limit=60, structured_logs=False)
        t2 = time.monotonic()
        yf_fin = yfc2.get_financials(kind="income_stmt", period="annual", tickers=yf_tickers_fin, connect_db=db_path, show_progress=False)
        t3 = time.monotonic()
        entry["measurements"]["yfinance_financials"] = {
            "duration_s": round(t3 - t2, 3),
            "metrics": {
                "summary": getattr(yf_fin, "metrics_summary", {}),
                "counters": getattr(yf_fin, "metrics_counters", {}),
                "errors": getattr(yf_fin, "errors", []),
            },
        }
        if sh_key:
            try:
                shc = SharadarClient(api_key=sh_key, rate_limit=60, structured_logs=False)
                t4 = time.monotonic()
                sh_fin = shc.get_fundamental_data(tickers=sh_tickers, connect_db=db_path, dimension="MRT", start_date=sh_start, end_date=sh_end)
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
                entry["measurements"]["sharadar_sf1_mrt"] = {"error": str(e)}
        results["runs"].append(entry)
    def _score(r: dict[str, Any], run_key: str) -> float:
        m = r["measurements"].get(run_key, {})
        duration = m.get("duration_s", float("inf"))
        metrics = m.get("metrics", {})
        errors = metrics.get("errors", [])
        err_cnt = len(errors) if isinstance(errors, list) else 0
        if rank_by == "duration":
            return float(duration) + float(err_cnt) * float(rank_error_penalty)
        summary = metrics.get("summary", {})
        p95 = summary.get("http_duration_s_p95", duration)
        try:
            return float(duration) + float(rank_alpha) * float(p95) + float(err_cnt) * float(rank_error_penalty)
        except Exception:
            return float(duration) + float(err_cnt) * float(rank_error_penalty)
    def _best(run_key: str) -> dict[str, Any]:
        ranked = sorted(results["runs"], key=lambda r: _score(r, run_key))
        return ranked[0] if ranked else {}
    results["best"] = {"yfinance_price": _best("yfinance_price"), "yfinance_financials": _best("yfinance_financials")}
    report_path.write_text(json.dumps(results, indent=2))
    best_path = out_dir / "profile_tuning_best.json"
    best_path.write_text(json.dumps(results["best"], indent=2))
    click.echo(str(report_path))
    if db_path.exists():
        db_path.unlink()

@tune.command("export-best")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--write-file", type=click.Path(path_type=Path), default=None)
def tune_export_best(output_dir: Path | None, write_file: Path | None) -> None:
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    best_path = out_dir / "profile_tuning_best.json"
    if not best_path.exists():
        click.echo(f"# best file not found: {best_path}")
        return
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
        Path(write_file).write_text(content)
        click.echo(str(write_file))
    else:
        click.echo(content)

if __name__ == "__main__":
    main()
