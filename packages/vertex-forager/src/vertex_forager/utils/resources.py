from __future__ import annotations

from typing import TYPE_CHECKING
import warnings

import psutil

if TYPE_CHECKING:
    from pathlib import Path


def validate_memory_usage(
    symbols: list[str] | None,
    connect_db: str | Path | None,
    bytes_per_item: int = 1 * 1024 * 1024,
    threshold_ratio: float = 0.7,
    threshold_absolute: int | None = 4 * 1024 * 1024 * 1024,
    estimated_count: int | None = None,
) -> None:
    if connect_db is not None:
        return
    if symbols is None:
        if estimated_count is None:
            return
        if estimated_count < 0:
            raise ValueError("estimated_count must be non-negative")
        num_items = estimated_count
    else:
        num_items = len(symbols)
    if not isinstance(bytes_per_item, int) or bytes_per_item <= 0:
        raise ValueError("bytes_per_item must be a positive integer")
    estimated_size = num_items * bytes_per_item
    available_memory = psutil.virtual_memory().available
    check_memory_safety(
        estimated_size,
        available_memory,
        num_items,
        threshold_ratio=threshold_ratio,
        threshold_absolute=threshold_absolute,
    )


def check_memory_safety(
    estimated_size: int,
    available_memory: int,
    num_tickers: int,
    threshold_ratio: float = 0.7,
    threshold_absolute: int | None = 4 * 1024 * 1024 * 1024,
) -> None:
    if estimated_size > available_memory * threshold_ratio:
        warnings.warn(
            f"High memory usage warning: Requesting data for {num_tickers} symbols "
            f"(est. {estimated_size / 1024 / 1024:.0f} MB) with only "
            f"{available_memory / 1024 / 1024:.0f} MB available. "
            "Consider using 'connect_db' to save to disk.",
            UserWarning,
            stacklevel=3,
        )
    elif threshold_absolute is not None and estimated_size > threshold_absolute:
        warnings.warn(
            f"Large data request warning: Requesting data for {num_tickers} symbols "
            f"(est. {estimated_size / 1024 / 1024 / 1024:.1f} GB). "
            "This may impact system performance. "
            "Consider using 'connect_db' to save to disk.",
            UserWarning,
            stacklevel=3,
        )
