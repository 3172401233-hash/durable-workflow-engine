"""Durable workflow engine.

All visible workflow state is derived from a committed, append-only event
log. External side effects are performed through an injected executor, never
by the engine itself.
"""

from .log import EventLog
from .engine import (
    ActivityFailedError,
    ActivityTimeout,
    RemoteError,
    WorkflowCancelled,
    WorkflowEngine,
    WorkflowTimedOut,
)
from .state import fold_state

__all__ = [
    "ActivityFailedError",
    "ActivityTimeout",
    "EventLog",
    "RemoteError",
    "WorkflowCancelled",
    "WorkflowEngine",
    "WorkflowTimedOut",
    "fold_state",
]
