from __future__ import annotations

from typing import Mapping


def filter_reserved_kwargs(kwargs: Mapping[str, object], reserved: set[str]) -> dict[str, object]:
    """Filter out reserved pipeline kwargs that are passed explicitly.
    
    Args:
        kwargs: Original kwargs mapping.
        reserved: Keys to remove from forwarding to pipeline.run.
    
    Returns:
        A new dict without reserved keys.
    """
    return {k: v for k, v in kwargs.items() if k not in reserved}
