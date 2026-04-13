"""
Domain exceptions for WCBT.

Notes
-----
WCBT intentionally avoids raising generic exceptions from core engine logic.
All expected failure modes should map to a domain exception with clear meaning.
"""

from __future__ import annotations


class WcbtError(RuntimeError):
    """Base exception for all WCBT domain failures."""


class BackupError(WcbtError):
    """Raised when a backup operation cannot be planned or executed."""


class BackupCommitNotImplementedError(BackupError):
    """Raised when a non-dry-run backup is requested but not implemented."""


class BackupMaterializationError(BackupError):
    """Raised when a backup run cannot be materialized safely."""


class BackupExecutionError(BackupError):
    """Raised when executing a backup plan fails."""


class BackupInvariantViolationError(BackupExecutionError):
    """Raised when an execution-time invariant is violated."""


class SchedulingError(WcbtError):
    """Base exception for Windows scheduling failures."""


class InvalidScheduleError(SchedulingError):
    """Raised when a requested schedule is invalid or unsupported."""


class ScheduledTaskNotFoundError(SchedulingError):
    """Raised when a requested Windows scheduled task is not present."""


class SchedulingBackendError(SchedulingError):
    """Raised when the schtasks backend reports an unexpected failure."""
