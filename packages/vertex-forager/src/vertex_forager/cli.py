from __future__ import annotations

from collections.abc import Mapping
import contextlib
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import TYPE_CHECKING, Any, Literal, cast

import click
import httpx

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
    get_state_db_path,
    list_dlq_entries,
)
from vertex_forager.core.sweep import (
    build_sweep_combinations,
    run_sweep_measurements,
    score_and_rank_results,
)
from vertex_forager.state import StateManager

from .utils import cleanup_dlq_tmp, clear_app_cache, get_app_root, get_cache_dir

if TYPE_CHECKING:
    from vertex_forager.clients.base import BaseClient
    from vertex_forager.core.config import RunResult

logger = logging.getLogger(__name__)
YF_PROFILE_DEFAULT_TICKERS = "AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,NFLX,ADBE,CSCO"


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

def _create_cli_client(
    *,
    provider: str,
    quality_check: Literal["warn", "error"] = "warn",
) -> BaseClient:
    if provider == "sharadar":
        return cast(
            "BaseClient",
            create_client(provider="sharadar", rate_limit=DEFAULT_RATE_LIMIT, quality_check=quality_check),
        )
    if provider == "yfinance":
        return cast("BaseClient", create_client(provider="yfinance", quality_check=quality_check))
    raise click.ClickException(f"Unsupported provider: {provider}")


def _print_run_result_summary(result: RunResult) -> None:
    total_rows = sum(result.tables.values()) if isinstance(result.tables, Mapping) else 0
    click.echo(
        " ".join(
            [
                "✅ Run completed",
                f"provider={result.provider}",
                f"dataset={result.dataset or '-'}",
                f"rows={total_rows}",
                f"errors={len(result.errors)}",
                f"duration_s={result.duration_s or 0:.3f}",
            ]
        )
    )
    for table_name, row_count in result.tables.items():
        quality_count = result.quality_violations.get(table_name, 0)
        dlq_counts = result.dlq_counts.get(table_name, {})
        rescued = int(dlq_counts.get("rescued", 0))
        remaining = int(dlq_counts.get("remaining", 0))
        click.echo(
            " ".join(
                [
                    f"table={table_name}",
                    f"rows={row_count}",
                    f"quality_violations={quality_count}",
                    f"dlq_rescued={rescued}",
                    f"dlq_remaining={remaining}",
                ]
            )
        )
    for error in result.errors:
        click.echo(
            " ".join(
                [
                    "error",
                    f"symbol={error.symbol or '-'}",
                    f"type={error.exc_type}",
                    f"retryable={error.retryable}",
                    f"message={error.message}",
                ]
            )
        )


def _collect_common_options(func: Any) -> Any:
    func = click.option("--no-progress", is_flag=True, default=False, help="Suppress progress output")(func)
    func = click.option(
        "--quality-check",
        type=click.Choice(["warn", "error"]),
        default="warn",
        show_default=True,
        help="Quality violation handling mode",
    )(func)
    func = click.option("--output", type=str, default=None, help="DuckDB output URI, e.g. duckdb:///data.db")(func)
    return func


def _run_collection(
    *,
    provider: str,
    method_name: str,
    quality_check: str,
    kwargs: dict[str, Any],
) -> None:
    try:
        client = _create_cli_client(
            provider=provider,
            quality_check=cast("Literal['warn', 'error']", quality_check),
        )
        method = getattr(client, method_name)
        result = cast("RunResult", method(**kwargs))
        _print_run_result_summary(result)
    except click.ClickException:
        raise
    except (ValueError, KeyError, TypeError) as exc:
        raise click.ClickException(str(exc)) from None
    except httpx.RequestError:
        raise click.ClickException("Collection error: network request failed.") from None


def _emit_table_counts(title: str, rows: list[tuple[str, str]]) -> None:
    click.echo(title)
    if not rows:
        click.echo("  - none")
        return
    for table_name, value in rows:
        click.echo(f"  - {table_name}: {value}")


def _load_status_rows(state_db: Path) -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[tuple[str, str]]]:
    if not state_db.exists():
        return [], [], []
    with sqlite3.connect(state_db) as conn:
        checkpoint_rows = [
            (str(row[0] or "-"), str(int(row[1])))
            for row in conn.execute(
                """
                SELECT table_name, COUNT(*)
                FROM checkpoints
                WHERE status != 'completed'
                GROUP BY table_name
                ORDER BY table_name
                """
            ).fetchall()
        ]
        dlq_rows = [
            (str(row[0] or "-"), str(int(row[1])))
            for row in conn.execute(
                """
                SELECT table_name, COUNT(*)
                FROM dlq_index
                WHERE status = 'pending'
                GROUP BY table_name
                ORDER BY table_name
                """
            ).fetchall()
        ]
        last_run_rows = [
            (str(row[0] or "-"), _format_timestamp(row[1]))
            for row in conn.execute(
                """
                SELECT table_name, MAX(created_at)
                FROM run_history
                GROUP BY table_name
                ORDER BY table_name
                """
            ).fetchall()
        ]
    return checkpoint_rows, dlq_rows, last_run_rows


def _resolve_dlq_replay_output(*, table: str, output: str | None) -> str:
    if output is not None:
        return output
    entries = list_dlq_entries(table=table, status="pending")
    if not entries:
        raise click.ClickException(f"No pending DLQ entries for table '{table}'.")
    output_uris = sorted(
        {
            str(entry["output_uri"])
            for entry in entries
            if isinstance(entry.get("output_uri"), str) and str(entry["output_uri"]).strip()
        }
    )
    if not output_uris:
        raise click.ClickException(f"No stored output destination for table '{table}'. Provide --output.")
    if len(output_uris) > 1:
        raise click.ClickException(f"Multiple stored output destinations found for table '{table}'. Provide --output.")
    return output_uris[0]


def _provider_from_table(table: str) -> str:
    provider = table.split("_", 1)[0]
    if provider not in {"sharadar", "yfinance"}:
        raise click.ClickException(f"Unable to infer provider from table '{table}'.")
    return provider


@main.group()
def collect() -> None:
    """Collect provider datasets using task-oriented subcommands."""
    pass


@collect.group()
def sharadar() -> None:
    """Sharadar dataset collection commands."""
    pass


@collect.group()
def yfinance() -> None:
    """YFinance dataset collection commands."""
    pass


@sharadar.command("price")
@click.option("--symbol", "-s", "symbols", multiple=True, required=True)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
@_collect_common_options
def collect_sharadar_price(
    symbols: tuple[str, ...],
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="sharadar",
        method_name="get_price_data",
        quality_check=quality_check,
        kwargs={
            "tickers": list(symbols),
            "connect_db": output,
            "start_date": start_date,
            "end_date": end_date,
            "progress": not no_progress,
        },
    )


@sharadar.command("fundamentals")
@click.option("--symbol", "-s", "symbols", multiple=True, required=True)
@click.option(
    "--dimension",
    type=click.Choice(["MRY", "MRQ", "MRT", "ARY", "ARQ", "ART"]),
    default="MRY",
    show_default=True,
)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
@_collect_common_options
def collect_sharadar_fundamentals(
    symbols: tuple[str, ...],
    dimension: str,
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="sharadar",
        method_name="get_fundamental_data",
        quality_check=quality_check,
        kwargs={
            "tickers": list(symbols),
            "dimension": dimension,
            "connect_db": output,
            "start_date": start_date,
            "end_date": end_date,
            "progress": not no_progress,
        },
    )


@sharadar.command("tickers")
@_collect_common_options
def collect_sharadar_tickers(
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="sharadar",
        method_name="get_ticker_info",
        quality_check=quality_check,
        kwargs={
            "connect_db": output,
            "progress": not no_progress,
        },
    )


@sharadar.command("sp500")
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
@_collect_common_options
def collect_sharadar_sp500(
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="sharadar",
        method_name="get_sp500_history",
        quality_check=quality_check,
        kwargs={
            "connect_db": output,
            "start_date": start_date,
            "end_date": end_date,
            "progress": not no_progress,
        },
    )


@yfinance.command("price")
@click.option("--symbol", "-s", "symbols", multiple=True, required=True)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
@_collect_common_options
def collect_yfinance_price(
    symbols: tuple[str, ...],
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="yfinance",
        method_name="get_price_data",
        quality_check=quality_check,
        kwargs={
            "tickers": list(symbols),
            "connect_db": output,
            "start_date": start_date,
            "end_date": end_date,
            "progress": not no_progress,
        },
    )


@yfinance.command("financials")
@click.option("--symbol", "-s", "symbols", multiple=True, required=True)
@click.option(
    "--kind",
    type=click.Choice(["income_stmt", "balance_sheet", "cashflow"]),
    required=True,
)
@click.option(
    "--period",
    type=click.Choice(["annual", "quarterly"]),
    default="annual",
    show_default=True,
)
@_collect_common_options
def collect_yfinance_financials(
    symbols: tuple[str, ...],
    kind: str,
    period: str,
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="yfinance",
        method_name="get_financials",
        quality_check=quality_check,
        kwargs={
            "tickers": list(symbols),
            "kind": kind,
            "period": period,
            "connect_db": output,
            "progress": not no_progress,
        },
    )


@yfinance.command("info")
@click.option("--symbol", "-s", "symbols", multiple=True, required=True)
@_collect_common_options
def collect_yfinance_info(
    symbols: tuple[str, ...],
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="yfinance",
        method_name="get_info",
        quality_check=quality_check,
        kwargs={
            "tickers": list(symbols),
            "connect_db": output,
            "progress": not no_progress,
        },
    )


@yfinance.command("dividends")
@click.option("--symbol", "-s", "symbols", multiple=True, required=True)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
@_collect_common_options
def collect_yfinance_dividends(
    symbols: tuple[str, ...],
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    quality_check: str,
    no_progress: bool,
) -> None:
    _run_collection(
        provider="yfinance",
        method_name="get_actions",
        quality_check=quality_check,
        kwargs={
            "tickers": list(symbols),
            "kind": "dividends",
            "connect_db": output,
            "start_date": start_date,
            "end_date": end_date,
            "progress": not no_progress,
        },
    )


@main.command()
def status() -> None:
    """Check current data storage and persisted state summaries."""
    root = get_app_root()
    state_db = get_state_db_path()
    click.echo(f"📂 Data Root: {root}")
    click.echo(f"📦 Cache Dir: {get_cache_dir()}")
    click.echo(f"🗄️ State DB: {state_db}")
    size = 0
    for path in root.glob("**/*"):
        try:
            if path.is_file():
                size += path.stat().st_size
        except (PermissionError, OSError) as exc:
            logger.warning("Failed to stat file during size calculation: %s", exc)
    click.echo(f"📊 Total Data Size: {size / (1024 * 1024):.2f} MB")
    checkpoint_rows, dlq_rows, last_run_rows = _load_status_rows(state_db)
    _emit_table_counts("Checkpoint entries per table", checkpoint_rows)
    _emit_table_counts("Pending DLQ batches per table", dlq_rows)
    _emit_table_counts("Last run timestamp per table", last_run_rows)


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
@click.option("--table", type=str, default=None, help="Filter by table name")
def runs_list(limit: int, table: str | None) -> None:
    """List recent run-history rows from SQLite."""
    records = StateManager().runs.list(table=table, limit=limit)
    if not records:
        click.echo("No run history found.")
        return
    for record in records:
        click.echo(
            " ".join(
                [
                    f"run_id={record.run_id}",
                    f"provider={record.provider}",
                    f"dataset={record.dataset or '-'}",
                    f"table={record.table_name or '-'}",
                    f"rows={sum(record.tables.values())}",
                    f"errors={record.error_count}",
                    f"finished_at={_format_timestamp(record.finished_at)}",
                ]
            )
        )


@runs.command("clear")
@click.option("--table", type=str, default=None, help="Filter by table name")
@click.option("--before", required=False, help="Delete run history older than this window, e.g. 30d")
def runs_clear(table: str | None, before: str | None) -> None:
    """Delete run history older than the provided window."""
    if table is None and before is None:
        raise click.UsageError("Provide --table, --before, or both.")
    before_days = _parse_before_days(before) if before is not None else None
    deleted = StateManager().runs.clear(table=table, before_days=before_days)
    click.echo(f"🧹 Deleted {deleted} run history rows.")


@main.group()
def dlq() -> None:
    """Inspect and manage DLQ spool metadata."""
    pass


@dlq.command("list")
@click.option("--table", type=str, default=None, help="Filter by table name")
@click.option(
    "--status",
    type=click.Choice(["pending", "recovered", "all"]),
    default="pending",
    show_default=True,
    help="Filter by DLQ status",
)
def dlq_list(table: str | None, status: str) -> None:
    """List pending DLQ entries from the SQLite index."""
    state = StateManager()
    entries = state.dlq.list(table=table, status=None if status == "all" else status)
    if not entries:
        click.echo("No DLQ entries found.")
        return
    for entry in entries:
        click.echo(
            " ".join(
                [
                    f"table={entry.table}",
                    f"status={entry.status}",
                    f"rows={entry.row_count}",
                    f"created_at={_format_timestamp(entry.created_at.timestamp())}",
                    f"path={entry.path.name}",
                    f"retries={entry.retry_count}",
                ]
            )
        )


@dlq.command("replay")
@click.option("--table", required=True, help="Replay pending DLQ entries for this table")
@click.option("--output", type=str, default=None, help="Override the replay output URI")
@click.option("--dry-run", is_flag=True, default=False, help="Enumerate entries without writing")
def dlq_replay(table: str, output: str | None, dry_run: bool) -> None:
    """Replay pending DLQ entries using the persisted DLQ index."""
    state = StateManager()
    resolved_output = output or ("duckdb:///__dry_run__.duckdb" if dry_run else None)
    if resolved_output is None:
        resolved_output = _resolve_dlq_replay_output(table=table, output=None)
    result = state.dlq.replay(table=table, output=resolved_output, dry_run=dry_run)
    click.echo(
        " ".join(
            [
                "✅ DLQ replay finished",
                f"table={table}",
                f"replayed={result.replayed}",
                f"failed={result.failed}",
                f"skipped={result.skipped}",
            ]
        )
    )
    for error in result.errors:
        click.echo(f"error={error}")


@dlq.command("clear")
@click.option("--table", type=str, default=None, help="Clear DLQ entries for this table")
@click.option("--all", "clear_all", is_flag=True, default=False, help="Clear all DLQ entries")
def dlq_clear(table: str | None, clear_all: bool) -> None:
    """Delete DLQ entries by table or clear everything with confirmation."""
    if not clear_all and table is None:
        raise click.UsageError("Provide --table or --all.")
    if clear_all and not click.confirm("⚠️ Delete all DLQ state?"):
        return
    deleted = StateManager().dlq.clear(table=None if clear_all else table)
    click.echo(f"🧹 Deleted {deleted} DLQ rows.")


@main.group()
def checkpoints() -> None:
    """Resume or clear persisted checkpoints."""
    pass


@checkpoints.command("resume")
@click.option("--table", required=True, help="Resume the latest checkpoint for this table")
@click.option("--output", required=True, type=str, help="DuckDB output URI")
def checkpoints_resume(table: str, output: str) -> None:
    """Resume the latest checkpoint for the requested table."""
    provider = _provider_from_table(table)
    state = StateManager()
    client = _create_cli_client(provider=provider)
    result = state.checkpoints.resume(table=table, client=client, output=output)
    _print_run_result_summary(result)


@checkpoints.command("clear")
@click.option("--table", type=str, default=None, help="Clear checkpoints for this table")
@click.option("--all", "clear_all", is_flag=True, default=False, help="Clear all checkpoints")
def checkpoints_clear(table: str | None, clear_all: bool) -> None:
    """Clear checkpoints by table or all at once."""
    if not clear_all and table is None:
        raise click.UsageError("Provide --table or --all.")
    if clear_all and not click.confirm("⚠️ Delete all checkpoints?"):
        return
    deleted = StateManager().checkpoints.clear(table=None if clear_all else table)
    click.echo(f"🧹 Deleted {deleted} checkpoint rows.")


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
@click.option("--tickers", type=str, default=YF_PROFILE_DEFAULT_TICKERS, show_default=True)
@click.option("--start-date", type=str, default=None)
@click.option("--end-date", type=str, default=None)
def tune_profile(
    kind: str,
    output_dir: Path | None,
    tickers: str,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """Run a single profiling run for price or financials data.

    Args:
        kind: Type of profile ('price' or 'financials').
        output_dir: Directory for result JSON. If None, uses VF_PROFILE_OUTPUT_DIR or default path.
        tickers: Comma-separated list of ticker symbols (e.g., 'AAPL,MSFT').
        start_date: Start date for price data (YYYY-MM-DD). If None, uses provider default.
        end_date: End date for price data (YYYY-MM-DD). If None, uses provider default.

    Returns:
        None: Results are saved to a JSON file and the path is printed to stdout.
    """
    out_dir = output_dir or Path(os.getenv("VF_PROFILE_OUTPUT_DIR") or (Path.cwd() / "output" / "forager-profiles"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ticker_tokens = tickers.split(",")
    cleaned_tickers = [token.strip().upper() for token in ticker_tokens if token.strip()]
    if len(cleaned_tickers) != len(ticker_tokens) or not cleaned_tickers:
        raise click.BadParameter(
            "tickers must be a comma-separated list of non-empty symbols",
            param_hint="--tickers",
        )
    if kind == "price":
        from vertex_forager.providers.yfinance.client import YFinanceClient
        from vertex_forager.utils import as_dict

        db_path = out_dir / "profile_run.duckdb"
        if db_path.exists():
            db_path.unlink()
        client = YFinanceClient(rate_limit=60)
        run = client.get_price_data(
            tickers=cleaned_tickers,
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
        yfc = YFinanceClient(rate_limit=60)
        yf_run = yfc.get_financials(
            kind="income_stmt",
            period="annual",
            tickers=cleaned_tickers,
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
                    tickers=cleaned_tickers[:5],
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


if __name__ == "__main__":
    main()
