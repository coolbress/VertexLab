import click
from .utils import get_app_root, get_cache_dir, clear_app_cache

@click.group()
def main():
    """Vertex Forager: High-performance Financial Data Collector"""
    # uv run으로 실행되므로 여기서 아키텍처 체크를 할 필요가 없습니다.
    pass

@main.command()
@click.option('--symbol', '-s', multiple=True, help="수집할 종목 코드 (예: AAPL)")
@click.option('--source', type=click.Choice(['yfinance', 'sharadar']), default='yfinance')
def collect(symbol, source):
    """지정한 소스로부터 금융 데이터를 수집합니다."""
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
        
        async def _run_collect():
            async with create_client(provider=source, api_key=api_key, rate_limit=120) as client:
                if source == "sharadar":
                    # For Sharadar, we use the specialized client method
                    # In the future, this can be generalized via a CollectorCore interface
                    result = await client.get_price_data(tickers=list(symbol))
                    return result
                else:
                    click.echo(f"⚠️ {source} is not fully implemented yet.")
                    return None

        result = asyncio.run(_run_collect())
        
        if result:
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

    except Exception as e:
        click.echo(f"❌ 수집 중 오류가 발생했습니다: {str(e)}")
        import traceback
        traceback.print_exc()

@main.command()
def status():
    """현재 데이터 저장소 및 시스템 상태를 확인합니다."""
    root = get_app_root()
    click.echo(f"📂 Data Root: {root}")
    click.echo(f"📦 Cache Dir: {get_cache_dir()}")
    
    # 데이터 용량 확인 등 유용한 정보 추가 가능
    size = sum(f.stat().st_size for f in root.glob('**/*') if f.is_file())
    click.echo(f"📊 Total Data Size: {size / (1024*1024):.2f} MB")

@main.command()
def clear():
    """모든 임시 캐시 데이터를 삭제합니다."""
    if click.confirm("⚠️ 모든 캐시 데이터를 삭제하시겠습니까?"):
        clear_app_cache()
        click.echo("🧹 캐시가 성공적으로 비워졌습니다.")

if __name__ == "__main__":
    main()