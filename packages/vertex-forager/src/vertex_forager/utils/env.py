from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def set_env(cfg: dict[str, Any]) -> None:
    for k, v in cfg.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = str(v)


def load_tickers_env(name: str, default: list[str]) -> list[str]:
    v = os.getenv(name)
    if not v:
        return list(default)
    toks = [t.strip().upper() for t in v.split(",") if t.strip()]
    return toks if toks else list(default)


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    truthy = ("1", "true", "yes", "on")
    falsy = ("0", "false", "no", "off")
    if s in truthy:
        return True
    if s in falsy:
        return False
    return default


def env_int(name: str, default: int | None = None) -> int | None:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float | None = None) -> float | None:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v.strip())
    except (TypeError, ValueError):
        return default


def load_env_file(env_file: Path | None = None) -> None:
    load_dotenv(dotenv_path=env_file, override=False)
