from typing import TYPE_CHECKING, Any

from vertex_forager.providers.sharadar.schema import (
    SHARADAR_ACTIONS,
    SHARADAR_DAILY,
    SHARADAR_SEP,
    SHARADAR_SF1,
    SHARADAR_SF2,
    SHARADAR_SF3,
    SHARADAR_SP500,
    SHARADAR_TICKERS,
)

if TYPE_CHECKING:
    from vertex_forager.providers.sharadar.client import SharadarClient
    from vertex_forager.providers.sharadar.router import SharadarRouter


def __getattr__(name: str) -> Any:
    if name == "SharadarClient":
        from vertex_forager.providers.sharadar.client import SharadarClient

        return SharadarClient
    if name == "SharadarRouter":
        from vertex_forager.providers.sharadar.router import SharadarRouter

        return SharadarRouter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "SharadarClient",
    "SharadarRouter",
    "SHARADAR_ACTIONS",
    "SHARADAR_DAILY",
    "SHARADAR_SEP",
    "SHARADAR_SF1",
    "SHARADAR_SF2",
    "SHARADAR_SF3",
    "SHARADAR_SP500",
    "SHARADAR_TICKERS",
]
