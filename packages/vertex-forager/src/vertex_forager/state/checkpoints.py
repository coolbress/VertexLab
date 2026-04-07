from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from vertex_forager.core.checkpoint import delete_checkpoints, find_latest_checkpoint
from vertex_forager.exceptions import CheckpointNotFoundError
from vertex_forager.utils import run_sync_compat
from vertex_forager.writers import create_writer
from vertex_forager.writers.memory import InMemoryBufferWriter

if TYPE_CHECKING:
    from vertex_forager.clients.base import BaseClient
    from vertex_forager.core.config import RunResult


def _client_provider(client: BaseClient[Any]) -> str:
    parts = client.__class__.__module__.split(".")
    if "providers" in parts:
        idx = parts.index("providers")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return client.__class__.__name__.removesuffix("Client").lower()


class CheckpointsNamespace:
    def resume(
        self,
        *,
        table: str,
        client: BaseClient[Any],
        output: str,
        **kwargs: Any,
    ) -> RunResult:
        if output.startswith("memory://"):
            raise ValueError("Checkpoint resume requires a persisted output destination")
        writer = create_writer(output)
        if isinstance(writer, InMemoryBufferWriter):
            raise ValueError("Checkpoint resume requires a persisted output destination")
        checkpoint = find_latest_checkpoint(table_name=table)
        if checkpoint is None:
            raise CheckpointNotFoundError(f"No checkpoint found for table '{table}'")
        provider = _client_provider(client)
        if checkpoint.provider != provider:
            raise ValueError(
                f"Checkpoint table '{table}' belongs to provider '{checkpoint.provider}', not '{provider}'"
            )
        resume_fn = getattr(client, "_resume_dataset_async", None)
        if resume_fn is None:
            raise TypeError(f"Client {type(client).__name__} does not support checkpoint resume")
        return cast(
            "RunResult",
            run_sync_compat(resume_fn(dataset=checkpoint.dataset, checkpoint=checkpoint, output=output, **kwargs)),
        )

    def clear(
        self,
        *,
        table: str | None = None,
    ) -> int:
        return delete_checkpoints(table_name=table)
