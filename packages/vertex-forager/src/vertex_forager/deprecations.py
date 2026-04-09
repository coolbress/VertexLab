from __future__ import annotations

import warnings


def warn_deprecated(
    *,
    old: str,
    new: str,
    deprecated_in: str,
    removed_in: str,
    stacklevel: int = 2,
) -> None:
    warnings.warn(
        (
            f"{old} is deprecated in {deprecated_in} and will be removed in {removed_in}; "
            f"use {new} instead."
        ),
        DeprecationWarning,
        stacklevel=stacklevel,
    )
