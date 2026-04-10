from __future__ import annotations

from collections.abc import Iterable
import re
from typing import Any

from vertex_forager.exceptions import RunError


def as_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    raw_errors_obj = getattr(obj, "errors", [])
    if raw_errors_obj is None:
        raw_errors: list[Any] = []
    elif isinstance(raw_errors_obj, (str, bytes, dict, RunError, BaseException)):
        raw_errors = [raw_errors_obj]
    elif isinstance(raw_errors_obj, Iterable):
        raw_errors = list(raw_errors_obj)
    else:
        raw_errors = [raw_errors_obj]
    errors: list[dict[str, Any] | str] = []
    for error in raw_errors:
        if isinstance(error, RunError):
            errors.append(
                {
                    "provider": error.provider,
                    "dataset": error.dataset,
                    "symbol": error.symbol,
                    "exc_type": error.exc_type,
                    "message": error.message,
                    "retryable": error.retryable,
                }
            )
        elif isinstance(error, BaseException):
            errors.append(str(error))
        else:
            errors.append(str(error))
    return {
        "counters": getattr(obj, "metrics_counters", {}),
        "histograms": getattr(obj, "metrics_histograms", {}),
        "summary": getattr(obj, "metrics_summary", {}),
        "tables": getattr(obj, "tables", {}),
        "errors": errors,
    }


def sanitize_field(v: object) -> str:
    s = "" if v is None else str(v)
    s = re.sub(r"\s+", "_", s)
    s = s.replace("=", "_")
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s
