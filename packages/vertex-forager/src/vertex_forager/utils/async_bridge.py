from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
import contextlib
import functools
from typing import Any, ParamSpec, Protocol, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


class _SupportsRunSyncCompat(Protocol):
    def _run_sync_compat(self, coro: Coroutine[Any, Any, R]) -> R: ...


def make_sync(async_func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, R]:
    @functools.wraps(async_func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        self = cast("_SupportsRunSyncCompat", args[0])
        return self._run_sync_compat(async_func(*args, **kwargs))

    return cast("Callable[P, R]", wrapper)


def run_sync_compat(coro: Coroutine[Any, Any, T]) -> T:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    try:
        import nest_asyncio
    except ImportError as exc:
        coro.close()
        raise RuntimeError(
            "Running inside an event loop. Reinstall or upgrade vertex-forager to include nest-asyncio support."
        ) from exc
    nest_asyncio.apply(loop)
    task = loop.create_task(coro)
    try:
        return loop.run_until_complete(task)
    except KeyboardInterrupt:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                loop.run_until_complete(task)
        raise
