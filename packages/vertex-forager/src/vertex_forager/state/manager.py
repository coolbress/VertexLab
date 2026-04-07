from __future__ import annotations

from pathlib import Path

from vertex_forager.core.checkpoint import get_state_db_path
from vertex_forager.state.checkpoints import CheckpointsNamespace
from vertex_forager.state.dlq import DLQNamespace
from vertex_forager.state.runs import RunsNamespace


class StateManager:
    """Credential-free entry point for persisted local state access."""

    def __init__(self) -> None:
        self._dlq = DLQNamespace()
        self._runs = RunsNamespace()
        self._checkpoints = CheckpointsNamespace()

    @property
    def db_path(self) -> Path:
        """Return the SQLite state database path used by state namespaces."""
        return get_state_db_path()

    @property
    def dlq(self) -> DLQNamespace:
        """Return the namespace for DLQ inspection and replay operations."""
        return self._dlq

    @property
    def runs(self) -> RunsNamespace:
        """Return the namespace for persisted run-history queries and cleanup."""
        return self._runs

    @property
    def checkpoints(self) -> CheckpointsNamespace:
        """Return the namespace for checkpoint resume and cleanup operations."""
        return self._checkpoints
