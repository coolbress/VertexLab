import click
import httpx
import logging
from typing import Any, Optional, cast
from pathlib import Path
from .utils import get_app_root, get_cache_dir, clear_app_cache

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
                provider=source, api_key=api_key, rate_limit=120
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
        "VF_DEFAULT_RATE_LIMIT": ("global", "DEFAULT_RATE_LIMIT"),
        "VF_MAX_CONNECTIONS": ("global", "HTTP_MAX_CONNECTIONS"),
    }
    env_vals = {
        "SHARADAR_API_KEY": os.getenv("SHARADAR_API_KEY"),
        **{k: os.getenv(k) for k in ENV_VAR_MAPPING.keys()},
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


if __name__ == "__main__":
    main()
