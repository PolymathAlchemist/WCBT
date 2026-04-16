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

from backup_engine.job_binding import JobBinding
from backup_engine.scheduling.models import BackupScheduleSpec
from backup_engine.template_policy import TemplateSelectionRules

JobId = str


@dataclass(frozen=True, slots=True)
class JobSummary:
    """
    A stable, minimal representation of a Job binding for GUI selection.

    Attributes
    ----------
    job_id:
        Stable identifier for the job.
    name:
        Human-friendly display name.
    """

    job_id: JobId
    name: str


class ProfileStore(Protocol):
    """
    Persistence API for authoritative Job binding plus Template policy.

    Implementations are engine-owned. The GUI should interact through this
    interface (typically via a GUI adapter that runs calls off the UI thread).

    """

    def list_jobs(self) -> Sequence[JobSummary]:
        """
        Return known active jobs in a stable display order.

        Returns
        -------
        Sequence[JobSummary]
            Active jobs ordered for display (implementation-defined but stable).
        """
        raise NotImplementedError

    def load_job_binding(self, job_id: JobId) -> JobBinding:
        """
        Load the authoritative Job-owned binding contract for a job.

        Parameters
        ----------
        job_id:
            Identifier of the Job binding to load.

        Returns
        -------
        JobBinding
            Authoritative Job meaning for the current live binding.

        Raises
        ------
        UnknownJobError
            If job_id is not present in the store.

        Notes
        -----
        This contract defines only Job identity, Template reference, and target
        binding. Policy and compatibility residue are intentionally excluded.
        """
        raise NotImplementedError

    def save_job_binding(self, binding: JobBinding) -> None:
        """
        Persist the authoritative Job-owned binding contract for a job.

        Parameters
        ----------
        binding:
            Authoritative Job binding to persist.

        Raises
        ------
        UnknownJobError
            If the target job does not exist.
        ValueError
            If required Job binding fields are empty.
        """
        raise NotImplementedError

    def create_job(self, name: str) -> JobId:
        """
        Create a new job.

        Parameters
        ----------
        name:
            Human-friendly display name.

        Returns
        -------
        JobId
            Newly created job identifier.
        """
        raise NotImplementedError

    def rename_job(self, job_id: JobId, new_name: str) -> None:
        """
        Rename an existing job.

        Parameters
        ----------
        job_id:
            Identifier of the job to rename.
        new_name:
            New display name.

        Raises
        ------
        UnknownJobError
            If job_id does not exist.
        """
        raise NotImplementedError

    def delete_job(self, job_id: JobId) -> None:
        """
        Delete a job.

        Implementations may perform a soft delete.

        Parameters
        ----------
        job_id:
            Identifier of the job to delete.

        Raises
        ------
        UnknownJobError
            If job_id does not exist.
        """
        raise NotImplementedError

    def load_template_selection_rules(self, job_id: JobId) -> TemplateSelectionRules:
        """
        Load the authoritative Template-owned selection rules for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job whose current bound Template policy should be
            loaded.

        Returns
        -------
        TemplateSelectionRules
            Template-authoritative include/exclude policy for the current job.

        Raises
        ------
        UnknownJobError
            If job_id is not present in the store.

        """
        raise NotImplementedError

    def save_template_selection_rules(
        self,
        job_id: JobId,
        name: str,
        selection_rules: TemplateSelectionRules,
    ) -> None:
        """
        Persist authoritative Template-owned selection rules for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job whose current bound Template policy should be
            updated.
        name:
            Display name to store for job listing.
        selection_rules:
            Template-authoritative include/exclude policy to persist.

        Raises
        ------
        InvalidRuleError
            If any pattern violates invariants.

        """
        raise NotImplementedError

    def load_template_compression(self, job_id: JobId) -> str:
        """
        Load the authoritative Template-owned compression policy for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job whose current bound Template policy should be
            loaded.

        Returns
        -------
        str
            Template-authoritative compression mode for the current job.

        Raises
        ------
        UnknownJobError
            If job_id is not present in the store.

        """
        raise NotImplementedError

    def save_template_compression(self, job_id: JobId, name: str, compression: str) -> None:
        """
        Persist authoritative Template-owned compression policy for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job whose current bound Template policy should be
            updated.
        name:
            Display name to store for job listing.
        compression:
            Template-authoritative compression policy to persist.

        Raises
        ------
        InvalidRuleError
            If the compression value violates current policy invariants.

        """
        raise NotImplementedError

    def load_backup_schedule(self, job_id: JobId) -> BackupScheduleSpec:
        """
        Load the persisted scheduled-task record for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job to load.

        Returns
        -------
        BackupScheduleSpec
            Persisted scheduled-task record.

        Raises
        ------
        UnknownJobError
            If the job or schedule is not present.
        """
        raise NotImplementedError

    def save_backup_schedule(self, schedule: BackupScheduleSpec) -> None:
        """
        Persist the scheduled-task record for a job.

        Parameters
        ----------
        schedule:
            Scheduled-task record to persist.

        Raises
        ------
        UnknownJobError
            If the target job does not exist.
        InvalidScheduleError
            If the schedule violates scheduler invariants.
        """
        raise NotImplementedError

    def delete_backup_schedule(self, job_id: JobId) -> None:
        """
        Delete the persisted backup schedule for a job.

        Parameters
        ----------
        job_id:
            Identifier of the job whose schedule should be removed.
        """
        raise NotImplementedError
