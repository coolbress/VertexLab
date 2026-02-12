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
    
    # TODO: Implement Collector Core logic here
    # Currently, this is a placeholder. 
    # See https://github.com/vertex-lab/issues/1 for the tracking issue.
    click.echo("⚠️ Collector Core integration is pending implementation.")
    
    click.echo("✅ 수집 프로세스가 완료되었습니다.")

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