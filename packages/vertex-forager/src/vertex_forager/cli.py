from __future__ import annotations

import asyncio
from collections.abc import Mapping
import contextlib
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import TYPE_CHECKING, Any, cast

import click
import httpx
import polars as pl

from vertex_forager.clients import create_client
from vertex_forager.constants import DEFAULT_RATE_LIMIT
from vertex_forager.constants_view import (
    build_constants_preview,
    render_constants_preview,
)
from vertex_forager.core.checkpoint import (
    delete_all_checkpoints,
    delete_all_dlq_entries,
    delete_all_run_history,
    delete_dlq_entries_before,
    delete_run_history_before,
    get_state_db_path,
    list_pending_dlq_entries,
    list_run_history,
    mark_dlq_retry_result,
)
from vertex_forager.core.config import FramePacket
from vertex_forager.core.recover import build_summary_message, execute_recovery
from vertex_forager.core.sweep import (
    build_sweep_combinations,
    run_sweep_measurements,
    score_and_rank_results,
)
from vertex_forager.writers.duckdb import DuckDBWriter

from .utils import cleanup_dlq_tmp, clear_app_cache, get_app_root, get_cache_dir

if TYPE_CHECKING:
    from vertex_forager.core.config import RunResult
    from vertex_forager.providers.sharadar.client import SharadarClient

logger = logging.getLogger(__name__)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    dir_path = path.parent
    dir_path.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(dir_path))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(path)
        with contextlib.suppress(Exception):
            dir_fd = os.open(str(dir_path), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except Exception:
        with contextlib.suppress(Exception):
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        raise


def _atomic_write_json(path: Path, payload: object) -> None:
    _atomic_write_bytes(path, json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"))


def _atomic_write_text(path: Path, content: str) -> None:
    _atomic_write_bytes(path, content.encode("utf-8"))


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
@click.option("--source", type=click.Choice(["yfinance", "sharadar"]), default="sharadar")
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
        # imports moved to module level to satisfy import ordering rules

        if source != "sharadar":
            raise click.ClickException(f"`{source}` is not supported by `collect` yet.")

        api_key_env = "SHARADAR_API_KEY"  # pragma: allowlist secret (env var name only)
        api_key = os.getenv(api_key_env)
        if not api_key:
            raise click.ClickException(f"Environment variable {api_key_env} is not set.")

        async def _run_collect() -> RunResult | None:
            async with create_client(provider="sharadar", api_key=api_key, rate_limit=DEFAULT_RATE_LIMIT) as client:
                sc = cast("SharadarClient", client)
                result = cast("RunResult | None", await sc._get_price_data_async(tickers=list(symbol)))
                return result

        result = asyncio.run(_run_collect())

        if result is not None:
            if hasattr(result, "tables") and isinstance(result.tables, Mapping):
                total_rows = sum(result.tables.values())
                click.echo(f"✅ Completed: processed {total_rows} rows.")
                for table, count in result.tables.items():
                    click.echo(f"  - {table}: {count} rows")
            elif result.data is not None:
                click.echo(f"✅ Completed: processed {len(result.data)} rows.")
            else:
                click.echo(f"✅ Completed: {result}")

    except click.ClickException:
        raise
    except (ValueError, KeyError) as e:
        logger.error("Collection failed: %s", e)
        raise click.ClickException(f"Collection error: {e}") from None
    except httpx.RequestError as e:
        logger.warning("Collection request failed: %s", type(e).__name__)
        raise click.ClickException("Collection error: network request failed.") from None
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
    state_db = get_state_db_path()
    click.echo(f"📂 Data Root: {root}")
    click.echo(f"📦 Cache Dir: {get_cache_dir()}")
    click.echo(f"🗄️ State DB: {state_db}")

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
    help="Show only environment-based entries (table format only)",
)
def constants(section: str, output_format: str, env_only: bool) -> None:
    """Preview centralized constants by section.

    Args:
        section: Constants section to render
        output_format: Output format (json/table)
        env_only: Filter table to show only environment-overridden keys
    """
    preview = build_constants_preview(section)
    rendered = render_constants_preview(
        preview=preview,
        output_format=output_format,
        env_only=env_only,
    )
    click.echo(rendered)


def _format_timestamp(value: object) -> str:
    """Format a UNIX timestamp for CLI output."""
    if value is None:
        return "-"
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")  # type: ignore[arg-type]
    except (TypeError, ValueError, OSError):
        return "-"


def _parse_before_days(value: str) -> int:
    """Parse CLI retention input of the form '<N>d'."""
    raw = value.strip().lower()
    if not raw.endswith("d"):
        raise click.BadParameter("Expected <N>d format, for example 30d")
    days_text = raw[:-1].strip()
    if not days_text.isdigit():
        raise click.BadParameter("Expected <N>d format, for example 30d")
    return int(days_text)


def _before_to_timestamp(value: str) -> float:
    """Convert a before-window string to a cutoff timestamp."""
    days = _parse_before_days(value)
    return datetime.now(tz=timezone.utc).timestamp() - (days * 86_400)


async def _retry_pending_dlq_entries(*, table_name: str, target_db: Path) -> tuple[int, int]:
    """Retry pending DLQ entries for a table against the target DuckDB."""
    entries = list_pending_dlq_entries(table_name)
    if not entries:
        return 0, 0
    writer = DuckDBWriter(target_db)
    succeeded_paths: list[Path] = []
    success_count = 0
    failure_count = 0
    close_error: Exception | None = None
    try:
        for entry in entries:
            path = Path(str(entry["path"]))
            try:
                if not path.exists():
                    raise FileNotFoundError(f"DLQ file not found: {path}")
                frame = pl.read_ipc(path)
                packet = FramePacket(
                    provider=str(entry.get("provider") or "dlq"),
                    table=str(entry["table"]),
                    frame=frame,
                    observed_at=datetime.now(timezone.utc),
                )
                await writer.write(packet)
                succeeded_paths.append(path)
            except Exception as exc:
                mark_dlq_retry_result(path=path, success=False, error=str(exc))
                failure_count += 1
    finally:
        try:
            await writer.close()
        except Exception as exc:
            close_error = exc
    if close_error is not None:
        for path in succeeded_paths:
            mark_dlq_retry_result(path=path, success=True, error=f"writer_close:{close_error}")
        raise click.ClickException(f"DLQ retry failed while closing writer: {close_error}") from None
    for path in succeeded_paths:
        try:
            path.unlink(missing_ok=True)
            mark_dlq_retry_result(path=path, success=True, error=None)
            success_count += 1
        except Exception as exc:
            logger.warning("DLQ retry payload cleanup failed for %s: %s", path, exc)
            mark_dlq_retry_result(path=path, success=True, error=f"delete_failed:{exc}")
            success_count += 1
    return success_count, failure_count


@main.command()
@click.option("--checkpoints", is_flag=True, default=False, help="Clear checkpoint state only")
@click.option("--runs", "clear_runs", is_flag=True, default=False, help="Clear run history only")
@click.option("--dlq", "clear_dlq", is_flag=True, default=False, help="Clear DLQ index and spool files only")
@click.option("--all", "clear_all", is_flag=True, default=False, help="Clear all cache state")
def clear(checkpoints: bool, clear_runs: bool, clear_dlq: bool, clear_all: bool) -> None:
    """Clear cached state selectively or remove the entire cache."""
    if not any((checkpoints, clear_runs, clear_dlq, clear_all)):
        clear_all = True
    if clear_all:
        if click.confirm("⚠️ Delete all cache data?"):
            clear_app_cache()
            click.echo("🧹 Cache cleared.")
        return
    selections: list[str] = []
    if checkpoints:
        selections.append("checkpoints")
    if clear_runs:
        selections.append("runs")
    if clear_dlq:
        selections.append("dlq")
    if not click.confirm(f"⚠️ Delete selected cache state: {', '.join(selections)}?"):
        return
    messages: list[str] = []
    if checkpoints:
        deleted = delete_all_checkpoints()
        messages.append(f"checkpoints={deleted}")
    if clear_runs:
        deleted = delete_all_run_history()
        messages.append(f"runs={deleted}")
    if clear_dlq:
        dlq_result = delete_all_dlq_entries()
        cleaned_tmp = cleanup_dlq_tmp(get_cache_dir() / "dlq", 0)
        messages.append(f"dlq_rows={dlq_result['rows']}")
        messages.append(f"dlq_files={dlq_result['files']}")
        messages.append(f"dlq_tmp={cleaned_tmp}")
    click.echo(f"🧹 Cache cleared: {' '.join(messages)}")


@main.group()
def runs() -> None:
    """Inspect and manage persisted run history."""
    pass


@runs.command("list")
@click.option("--limit", type=int, default=20, show_default=True, help="Maximum number of runs to show")
def runs_list(limit: int) -> None:
    """List recent run-history rows from SQLite."""
    entries = list_run_history(limit=limit)
    if not entries:
        click.echo("No run history found.")
        return
    for entry in entries:
        click.echo(
            " ".join(
                [
                    f"run_id={entry['run_id']}",
                    f"provider={entry['provider']}",
                    f"dataset={entry['dataset'] or '-'}",
                    f"rows={entry['total_rows']}",
                    f"errors={entry['error_count']}",
                    f"finished_at={_format_timestamp(entry['finished_at'])}",
                ]
            )
        )


@runs.command("clear")
@click.option("--before", required=True, help="Delete run history older than this window, e.g. 30d")
def runs_clear(before: str) -> None:
    """Delete run history older than the provided window."""
    cutoff = _before_to_timestamp(before)
    deleted = delete_run_history_before(cutoff)
    click.echo(f"🧹 Deleted {deleted} run history rows older than {before}.")


@main.group()
def dlq() -> None:
    """Inspect and manage DLQ spool metadata."""
    pass


@dlq.command("list")
def dlq_list() -> None:
    """List pending DLQ entries from the SQLite index."""
    entries = list_pending_dlq_entries()
    if not entries:
        click.echo("No pending DLQ entries.")
        return
    for entry in entries:
        click.echo(
            " ".join(
                [
                    f"table={entry['table']}",
                    f"rows={entry['row_count']}",
                    f"created_at={_format_timestamp(entry['created_at'])}",
                    f"path={Path(str(entry['path'])).name}",
                    f"retries={entry['retry_count']}",
                ]
            )
        )


@dlq.command("retry")
@click.option("--table", "table_name", required=True, help="Retry pending DLQ entries for this table")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Target DuckDB file path (or set VF_RECOVER_DB)",
)
def dlq_retry(table_name: str, db_path: Path | None) -> None:
    """Retry pending DLQ entries for a table."""
    target_db = _resolve_target_db(db_path, False)
    if target_db is None:
        raise click.ClickException("Missing target DB. Provide --db or set VF_RECOVER_DB")
    success_count, failure_count = asyncio.run(_retry_pending_dlq_entries(table_name=table_name, target_db=target_db))
    if success_count == 0 and failure_count == 0:
        click.echo(f"No pending DLQ entries for table {table_name}.")
        return
    click.echo(f"✅ DLQ retry finished: table={table_name} recovered={success_count} failed={failure_count}")


@dlq.command("clear")
@click.option("--before", required=True, help="Delete DLQ entries older than this window, e.g. 1d")
def dlq_clear(before: str) -> None:
    """Delete old DLQ index rows and their IPC files."""
    cutoff = _before_to_timestamp(before)
    deleted = delete_dlq_entries_before(cutoff)
    cleaned_tmp = cleanup_dlq_tmp(get_cache_dir() / "dlq", 0)
    click.echo(
        f"🧹 Deleted DLQ state older than {before}: rows={deleted['rows']} files={deleted['files']} tmp={cleaned_tmp}"
    )


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
@click.option("--kind", type=click.Choice(["price", "financials"]), default="price")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--tickers", type=str, default=None)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
def tune_profile(
    kind: str,
    output_dir: Path | None,
    tickers: str | None,
    start_date: str | None,
    end_date: str | None,
) -> None:
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
        client = YFinanceClient(rate_limit=60)
        _tickers = [t.strip().upper() for t in (tickers or "AAPL,MSFT,NVDA,GOOGL,AMZN").split(",")]
        run = client.get_price_data(
            tickers=_tickers,
            connect_db=db_path,
            progress=False,
            start_date=start_date,
            end_date=end_date,
        )
        data = as_dict(run)
        metrics_path = out_dir / "profile_metrics.json"
        _atomic_write_json(metrics_path, data)
        click.echo(str(metrics_path))
        if db_path.exists():
            db_path.unlink()
    else:
        from vertex_forager.providers.sharadar.client import SharadarClient
        from vertex_forager.providers.yfinance.client import YFinanceClient
        from vertex_forager.utils import as_dict

        db_path = out_dir / "profile_financials.duckdb"
        if db_path.exists():
            db_path.unlink()
        yf_tickers = [
            t.strip().upper()
            for t in (tickers or os.getenv("YF_TICKERS") or "AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,NFLX,ADBE,CSCO").split(
                ","
            )
        ]
        yfc = YFinanceClient(rate_limit=60)
        yf_run = yfc.get_financials(
            kind="income_stmt",
            period="annual",
            tickers=yf_tickers,
            connect_db=db_path,
            progress=False,
        )
        sh_key = os.getenv("SHARADAR_API_KEY")
        sh_run = None
        if sh_key:
            try:
                shc = SharadarClient(
                    api_key=sh_key,
                    rate_limit=60,
                )
                sh_run = shc.get_fundamental_data(
                    tickers=yf_tickers[:5],
                    connect_db=db_path,
                    dimension="MRT",
                )
            except Exception as e:
                logger.warning("Sharadar verification skipped due to error: %s", e)
                sh_run = None
        data = {
            "yfinance_financials": as_dict(yf_run),
            "sharadar_sf1_optional": as_dict(sh_run),
        }

        metrics_path = out_dir / "profile_financials_metrics.json"
        _atomic_write_json(metrics_path, data)
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
    try:
        combos, sweep_message = build_sweep_combinations(
            concurrency_list=concurrency_list,
            flush_rows_list=flush_rows_list,
            keepalive_list=keepalive_list,
            connections_list=connections_list,
            timeout_list=timeout_list,
            sample_count=sample_count,
            sample_seed=sample_seed,
        )
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from None
    click.echo(sweep_message)
    return combos


def _run_sweep_measurements(
    combos: list[dict[str, Any]],
    out_dir: Path,
    include_sharadar: bool,
) -> dict[str, Any]:
    return run_sweep_measurements(
        combos=combos,
        output_dir=out_dir,
        include_sharadar=include_sharadar,
        logger=logger,
    )


def _score_and_rank_results(
    results: dict[str, Any],
    rank_by: str,
    rank_alpha: float,
    rank_error_penalty: float,
) -> dict[str, Any]:
    return score_and_rank_results(
        results=results,
        rank_by=rank_by,
        rank_alpha=rank_alpha,
        rank_error_penalty=rank_error_penalty,
        logger=logger,
    )


@tune.command("sweep")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--include-sharadar", is_flag=True, default=False)
@click.option(
    "--concurrency-list",
    type=str,
    default=None,
    help="Comma-separated list of concurrency values",
)
@click.option(
    "--flush-rows-list",
    type=str,
    default=None,
    help="Comma-separated list of flush threshold rows",
)
@click.option(
    "--keepalive-list",
    type=str,
    default=None,
    help="Comma-separated list of HTTP keepalive counts",
)
@click.option(
    "--connections-list",
    type=str,
    default=None,
    help="Comma-separated list of HTTP max connections",
)
@click.option(
    "--timeout-list",
    type=str,
    default=None,
    help="Comma-separated list of HTTP timeouts (seconds)",
)
@click.option("--rank-by", type=click.Choice(["duration", "duration_p95"]), default="duration")
@click.option("--rank-alpha", type=float, default=0.4)
@click.option("--sample-count", type=int, default=None)
@click.option("--sample-seed", type=int, default=42)
@click.option("--rank-error-penalty", type=float, default=5.0)
def tune_sweep(
    output_dir: Path | None,
    include_sharadar: bool,
    concurrency_list: str | None,
    flush_rows_list: str | None,
    keepalive_list: str | None,
    connections_list: str | None,
    timeout_list: str | None,
    rank_by: str,
    rank_alpha: float,
    sample_count: int | None,
    sample_seed: int,
    rank_error_penalty: float,
) -> None:
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

    # 1. Build combinations
    combos = _build_sweep_combinations(
        concurrency_list,
        flush_rows_list,
        keepalive_list,
        connections_list,
        timeout_list,
        sample_count=sample_count,
        sample_seed=sample_seed,
    )

    # 2. Measure
    results = _run_sweep_measurements(combos, out_dir, include_sharadar)

    # 3. Score and Rank
    results = _score_and_rank_results(results, rank_by, rank_alpha, rank_error_penalty)

    _atomic_write_json(report_path, results)
    _atomic_write_json(best_path, results["best"])
    click.echo(str(report_path))


@tune.command("export-best")
@click.option("--output-dir", type=click.Path(path_type=Path), default=None)
@click.option("--write-file", type=click.Path(path_type=Path), default=None)
def tune_export_best(output_dir: Path | None, write_file: Path | None) -> None:
    """Export create_client(...) configuration snippets from best tuning results.

    Args:
        output_dir: Directory containing profile_tuning_best.json.
        write_file: Optional file path to write export commands to.

    Returns:
        None: Prints or writes Python snippets containing explicit client kwargs.
    """
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    best_path = out_dir / "profile_tuning_best.json"
    if not best_path.exists():
        raise click.ClickException(f"Best file not found: {best_path}")
    data = json.loads(best_path.read_text())
    lines: list[str] = []

    def _collect(label: str) -> None:
        entry = data.get(label) or {}
        client_config = entry.get("client_config") or {}
        lines.append(f"# {label} recommended create_client(...) overrides")
        if isinstance(client_config, dict) and client_config:
            rendered = json.dumps(client_config, indent=4, ensure_ascii=False)
            lines.append(f"{label} = {rendered}")
            lines.append(f"# create_client(..., **{label})")
        lines.append("")

    _collect("yfinance_price")
    _collect("yfinance_financials")
    content = "\n".join(lines)
    if write_file:
        _atomic_write_text(write_file, content)
        click.echo(str(write_file))
    else:
        click.echo(content)


def _resolve_recover_base(dlq_dir: Path | None) -> Path:
    base = dlq_dir or (get_cache_dir() / "dlq")
    if not base.exists():
        raise click.ClickException(f"DLQ directory not found: {base}")
    return base


def _resolve_target_db(db_path: Path | None, dry_run: bool) -> Path | None:
    env_db = os.getenv("VF_RECOVER_DB")
    target_db = db_path or (Path(env_db) if env_db and env_db.strip() else None)
    if not dry_run and target_db is None:
        raise click.ClickException("Missing target DB. Provide --db or set VF_RECOVER_DB")
    return target_db


def _select_recover_tables(base: Path, tables: tuple[str, ...]) -> list[str]:
    if tables:
        raw_tables = [t.strip() for t in tables if t and t.strip()]
        seen: set[str] = set()
        selected_tables: list[str] = []
        for table_name in raw_tables:
            if table_name not in seen:
                seen.add(table_name)
                selected_tables.append(table_name)
        return selected_tables
    return sorted([p.name for p in base.iterdir() if p.is_dir()])


def _build_recover_summary(*, base: Path, target_db: Path | None, dry_run: bool) -> dict[str, Any]:
    return {
        "base": str(base),
        "db": str(target_db) if target_db else None,
        "dry_run": dry_run,
        "tables": {},
        "errors": [],
        "error_counts": {},
    }


def _write_recover_report(*, report: Path | None, summary: dict[str, Any]) -> None:
    if report is None:
        return
    try:
        report.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(report, summary)
        click.echo(str(report))
    except Exception as exc:
        raise click.ClickException(f"Failed to write report: {exc}") from None


def _print_recover_details(summary: dict[str, Any]) -> None:
    for table_name, info in summary["tables"].items():
        details = info.get("details", [])
        for detail in details:
            click.echo(
                f"[detail] {table_name} file={detail.get('file')} "
                f"rows={detail.get('rows')} status={detail.get('status')}"
            )


@main.command("recover")
@click.option(
    "--dir",
    "dlq_dir",
    type=click.Path(path_type=Path),
    default=None,
    help="DLQ root directory (default: $ROOT/cache/dlq)",
)
@click.option("--table", "tables", multiple=True, help="Limit recovery to specific table(s)")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Target DuckDB file path (or set VF_RECOVER_DB)",
)
@click.option("--dry-run", is_flag=True, default=False, help="Scan and report without writing")
@click.option("--delete-on-success", is_flag=True, default=False, help="Delete IPC files after successful reinjection")
@click.option("--clean-tmp", is_flag=True, default=False, help="Remove stale .ipc.tmp files before recovery")
@click.option(
    "--retention-s",
    type=int,
    default=86_400,
    show_default=True,
    help="Retention window for .ipc.tmp cleanup (seconds)",
)
@click.option("--report", type=click.Path(path_type=Path), default=None, help="Write JSON report to this path")
@click.option("--progress", is_flag=True, default=False, help="Show per-file progress during recovery")
@click.option("--verbose", is_flag=True, default=False, help="Print per-file details without requiring --report")
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="Exit non-zero if any failures occur (RecoverFail/CloseFail/DeleteFail)",
)
def recover(
    dlq_dir: Path | None,
    tables: tuple[str, ...],
    db_path: Path | None,
    dry_run: bool,
    delete_on_success: bool,
    clean_tmp: bool,
    retention_s: int,
    report: Path | None,
    progress: bool,
    verbose: bool,
    strict: bool,
) -> None:
    """Recover failed DLQ batches into the target DuckDB database.

    Usage:
        vertex-forager recover --dir "$VERTEXFORAGER_ROOT/cache/dlq" \\
            --dry-run --report /tmp/dlq_report.json
        vertex-forager recover --table sharadar_sf1 \\
            --dir "$VERTEXFORAGER_ROOT/cache/dlq" --db /path/to/target.duckdb \\
            --delete-on-success
        vertex-forager recover --dir "$VERTEXFORAGER_ROOT/cache/dlq" \\
            --dry-run --strict --progress

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
        base = _resolve_recover_base(dlq_dir)
        if clean_tmp:
            try:
                deleted = cleanup_dlq_tmp(base, retention_s)
                click.echo(f"🧹 Cleaned {deleted} stale .ipc.tmp files")
            except ValueError as e:
                raise click.ClickException(str(e)) from None
        target_db = _resolve_target_db(db_path, dry_run)
        selected_tables = _select_recover_tables(base, tables)
        if not selected_tables:
            click.echo("No tables selected or found under DLQ.")
            return
        summary = _build_recover_summary(base=base, target_db=target_db, dry_run=dry_run)
        explicit_set = {t.strip() for t in tables if t and t.strip()}
        asyncio.run(
            execute_recovery(
                base=base,
                selected_tables=selected_tables,
                explicit_tables=explicit_set,
                target_db=target_db,
                dry_run=dry_run,
                delete_on_success=delete_on_success,
                progress=progress,
                summary=summary,
                echo=click.echo,
                logger=logger,
            )
        )
        _write_recover_report(report=report, summary=summary)
        click.echo(build_summary_message(summary))
        if verbose:
            _print_recover_details(summary)
        if strict and summary["errors"]:
            raise click.ClickException("Errors encountered during recovery. See --report for details.")
    except click.ClickException:
        raise
    except Exception as e:
        logger.exception("Unexpected recover error")
        raise click.ClickException(str(e)) from None


if __name__ == "__main__":
    main()
