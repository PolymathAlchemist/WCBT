"""
ProfileStore public API.

This module defines the minimal engine-owned persistence surface that the GUI
authoring experience is allowed to call. The GUI must not call the CLI and must
not depend on SQLite details; it should speak only in typed domain objects.

Notes
-----
- Patterns are treated as root-relative globs and should use '/' separators.
- Exclude precedence is an evaluation rule, not a storage transformation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

JobId = str


@dataclass(frozen=True, slots=True)
class JobSummary:
    """
    A stable, minimal representation of a job for GUI selection.

    Attributes
    ----------
    job_id:
        Stable identifier for the job.
    name:
        Human-friendly display name.
    """

    job_id: JobId
    name: str


@dataclass(frozen=True, slots=True)
class RuleSet:
    """
    Include and exclude patterns for a job.

    Attributes
    ----------
    include:
        Root-relative include globs using '/' separators.
    exclude:
        Root-relative exclude globs using '/' separators.
    """

    include: tuple[str, ...]
    exclude: tuple[str, ...]


class ProfileStore(Protocol):
    """
    Persistence API for per-job authoring rules.

    Implementations are engine-owned. The GUI should interact through this
    interface (typically via a GUI adapter that runs calls off the UI thread).
    """

    def list_jobs(self) -> Sequence[JobSummary]:
        """
        Return known jobs in a stable display order.

        Returns
        -------
        Sequence[JobSummary]
            Known jobs ordered for display (implementation-defined but stable).
        """
        raise NotImplementedError

    def load_rules(self, job_id: JobId) -> RuleSet:
        """
        Load rules for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job to load.

        Returns
        -------
        RuleSet
            Persisted rules for the job.

        Raises
        ------
        UnknownJobError
            If job_id is not present in the store.
        """
        raise NotImplementedError

    def save_rules(self, job_id: JobId, name: str, rules: RuleSet) -> None:
        """
        Persist rules for a job, creating the job if needed.

        Parameters
        ----------
        job_id:
            Identifier of the job to upsert.
        name:
            Display name to store for job listing.
        rules:
            Include/exclude patterns to persist.

        Raises
        ------
        InvalidRuleError
            If any pattern violates invariants.
        """
        raise NotImplementedError
