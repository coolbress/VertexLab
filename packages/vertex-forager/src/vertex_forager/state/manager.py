from __future__ import annotations

from pathlib import Path

from vertex_forager.core.checkpoint import get_state_db_path
from vertex_forager.state.checkpoints import CheckpointsNamespace
from vertex_forager.state.dlq import DLQNamespace
from vertex_forager.state.runs import RunsNamespace


class StateManager:
    def __init__(self) -> None:
        self._dlq = DLQNamespace()
        self._runs = RunsNamespace()
        self._checkpoints = CheckpointsNamespace()

    @property
    def db_path(self) -> Path:
        return get_state_db_path()

    @property
    def dlq(self) -> DLQNamespace:
        return self._dlq

    @property
    def runs(self) -> RunsNamespace:
        return self._runs

    @property
    def checkpoints(self) -> CheckpointsNamespace:
        return self._checkpoints
