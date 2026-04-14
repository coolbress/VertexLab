from __future__ import annotations

import json
import os
from typing import Any

from vertex_forager.providers.catalog import get_provider_constants_preview


def _global_constants_map(constants_mod: Any) -> dict[str, object]:
    return {
        "HTTP_MAX_CONNECTIONS": constants_mod.HTTP_MAX_CONNECTIONS,
        "HTTP_MAX_KEEPALIVE_CONNECTIONS": constants_mod.HTTP_MAX_KEEPALIVE_CONNECTIONS,
        "DEFAULT_RATE_LIMIT": constants_mod.DEFAULT_RATE_LIMIT,
        "WRITER_CHUNK_ROWS": constants_mod.WRITER_CHUNK_ROWS,
        "PRIORITY_PAGINATION": constants_mod.PRIORITY_PAGINATION,
        "PRIORITY_NEW_JOB": constants_mod.PRIORITY_NEW_JOB,
        "PRIORITY_SENTINEL": constants_mod.PRIORITY_SENTINEL,
        "PROGRESS_LOG_CHUNK_ROWS": constants_mod.PROGRESS_LOG_CHUNK_ROWS,
        "DEFAULT_TIME_ZONE": constants_mod.DEFAULT_TIME_ZONE,
    }


def _flow_constants_map(constants_mod: Any) -> dict[str, object]:
    return {
        "DEFAULT_AVG_LATENCY_S": constants_mod.DEFAULT_AVG_LATENCY_S,
        "CONCURRENCY_MIN": constants_mod.CONCURRENCY_MIN,
        "CONCURRENCY_MAX": constants_mod.CONCURRENCY_MAX,
        "GRADIENT_QUEUE_SIZE_DEFAULT": constants_mod.GRADIENT_QUEUE_SIZE_DEFAULT,
        "GRADIENT_SMOOTHING_DEFAULT": constants_mod.GRADIENT_SMOOTHING_DEFAULT,
        "GRADIENT_WINDOW_S": constants_mod.GRADIENT_WINDOW_S,
    }


def _queue_constants_map(constants_mod: Any) -> dict[str, object]:
    return {
        "QUEUE_TARGET_RAM_RATIO": constants_mod.QUEUE_TARGET_RAM_RATIO,
        "PACKET_SIZE_EST_BYTES": constants_mod.PACKET_SIZE_EST_BYTES,
        "QUEUE_MIN": constants_mod.QUEUE_MIN,
        "QUEUE_MAX": constants_mod.QUEUE_MAX,
        "QUEUE_DEFAULT": constants_mod.QUEUE_DEFAULT,
    }


def _collect_env_overrides() -> dict[str, object]:
    env_vals = {
        "SHARADAR_API_KEY": os.getenv("SHARADAR_API_KEY"),
    }
    env_overrides_obj: dict[str, object] = {k: v for k, v in env_vals.items() if v is not None}
    if "SHARADAR_API_KEY" in env_overrides_obj:
        env_overrides_obj["SHARADAR_API_KEY"] = "<redacted>"
    return env_overrides_obj


def build_constants_preview(section: str) -> dict[str, dict[str, object]]:
    from vertex_forager import constants as global_constants

    preview: dict[str, dict[str, object]] = {}
    provider_preview = get_provider_constants_preview()
    if section in ("global", "all"):
        preview["global"] = _global_constants_map(constants_mod=global_constants)
    if section in ("flow", "all"):
        preview["flow"] = _flow_constants_map(constants_mod=global_constants)
    if section in ("queue", "all"):
        preview["queue"] = _queue_constants_map(constants_mod=global_constants)
    if section in ("yfinance", "all"):
        preview["yfinance"] = provider_preview["yfinance"]
    if section in ("sharadar", "all"):
        preview["sharadar"] = provider_preview["sharadar"]
    if section in ("writers", "all"):
        preview["writers"] = {
            "WRITER_DUCKDB_MAX_WORKERS": global_constants.WRITER_DUCKDB_MAX_WORKERS,
            "WAL_AUTOCHECKPOINT_LIMIT": global_constants.WAL_AUTOCHECKPOINT_LIMIT,
        }
    env_overrides = _collect_env_overrides()
    if env_overrides:
        preview["env_overrides"] = env_overrides
    return preview


def render_constants_preview(
    *,
    preview: dict[str, dict[str, object]],
    output_format: str,
    env_only: bool,
) -> str:
    if env_only:
        preview = {"env_overrides": preview.get("env_overrides", {})}
    if output_format == "json":
        return json.dumps(preview, indent=2, ensure_ascii=False)
    lines: list[str] = []
    for name, values in preview.items():
        keys = list(values.keys())
        if env_only and name != "env_overrides":
            continue
        if not keys:
            continue
        lines.append(f"\n[{name}]")
        max_key = max((len(k) for k in keys), default=0)
        for key in keys:
            lines.append(f"{key.ljust(max_key)}  :  {values[key]}")
    return "\n".join(lines)
