import click
import httpx
import logging
from typing import Any, Optional
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
    # uv run으로 실행되므로 여기서 아키텍처 체크를 할 필요가 없습니다.
    pass


@main.command()
@click.option("--symbol", "-s", multiple=True, help="수집할 종목 코드 (예: AAPL)")
@click.option(
    "--source", type=click.Choice(["yfinance", "sharadar"]), default="yfinance"
)
def collect(symbol: tuple[str, ...], source: str) -> None:
    """
    지정한 소스로부터 금융 데이터를 수집합니다.

    Args:
        symbol: 수집할 종목 코드 리스트 (예: ('AAPL', 'MSFT')).
        source: 데이터 소스 ('yfinance' 또는 'sharadar').

    Returns:
        None: 수집 결과는 로그로 출력되거나 파일로 저장됩니다.

    Raises:
        ValueError: API 키가 누락되었거나 잘못된 입력이 제공된 경우.
        httpx.RequestError: 네트워크 요청 실패 시.
    """
    if not symbol:
        click.echo("⚠️ 수집할 종목(--symbol)을 입력해주세요.")
        return

    click.echo(f"🚀 {source}를 통해 {symbol} 데이터 수집을 시작합니다...")

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
                click.echo(f"⚠️ {api_key_env} 환경변수가 설정되지 않았습니다.")
                return

        async def _run_collect() -> Optional[Any]:
            async with create_client(
                provider=source, api_key=api_key, rate_limit=120
            ) as client:
                if source == "sharadar":
                    # For Sharadar, we use the specialized client method
                    # In the future, this can be generalized via a CollectorCore interface
                    result = await client.get_price_data(tickers=list(symbol))
                    return result
                else:
                    raise NotImplementedError(f"{source} is not fully implemented yet.")

        result = asyncio.run(_run_collect())

        if result is not None:
            # Show summary
            if hasattr(result, "tables") and isinstance(result.tables, Mapping):
                total_rows = sum(result.tables.values())
                click.echo(f"✅ 수집 완료: 총 {total_rows}개 행이 처리되었습니다.")
                for table, count in result.tables.items():
                    click.echo(f"  - {table}: {count} rows")
            elif isinstance(result, pl.DataFrame):
                click.echo(f"✅ 수집 완료: 총 {len(result)}개 행이 처리되었습니다.")
            else:
                # Fallback for other result types
                click.echo(f"✅ 수집 완료: {result}")

    except (ValueError, KeyError, httpx.RequestError) as e:
        # Expected errors
        click.echo(f"❌ 수집 중 오류가 발생했습니다: {str(e)}")
        logger.error(f"Collection failed: {e}")
    except Exception as e:
        # Unexpected errors
        click.echo(f"❌ 예상치 못한 오류가 발생했습니다: {str(e)}")
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

    # 데이터 용량 확인 등 유용한 정보 추가 가능
    size = 0
    for f in root.glob("**/*"):
        try:
            if f.is_file():
                size += f.stat().st_size
        except Exception:
            # Skip files that cannot be accessed
            continue

    click.echo(f"📊 Total Data Size: {size / (1024 * 1024):.2f} MB")


@main.command()
def clear() -> None:
    """Clear all temporary cache data.

    Args:
        None

    Returns:
        None: Confirmation message is printed to stdout.
    """
    if click.confirm("⚠️ 모든 캐시 데이터를 삭제하시겠습니까?"):
        clear_app_cache()
        click.echo("🧹 캐시가 성공적으로 비워졌습니다.")


if __name__ == "__main__":
    main()
