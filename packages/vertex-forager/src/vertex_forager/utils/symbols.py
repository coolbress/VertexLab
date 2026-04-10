from __future__ import annotations

from vertex_forager.exceptions import InputError


def process_symbols(tickers: list[str] | None) -> list[str] | None:
    if tickers is not None:
        return [t.strip().upper() for t in tickers if t and t.strip()]
    return None


def validate_tickers(symbols: list[str] | tuple[str, ...]) -> None:
    if not isinstance(symbols, (list, tuple)):
        raise InputError("tickers must be a list or tuple of strings")
    if len(symbols) == 0:
        raise InputError("tickers list cannot be empty")
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise InputError("each ticker must be a string")
        stripped = symbol.strip()
        if not stripped or stripped != symbol:
            raise InputError("tickers must be non-empty and must not include leading/trailing whitespace")
