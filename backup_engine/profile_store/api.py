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

from backup_engine.scheduling.models import BackupScheduleSpec
from backup_engine.template_policy import TemplatePolicy, TemplateSelectionRules

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
    Transitional Job-shaped carrier for Template-owned selection rules.

    Attributes
    ----------
    include:
        Root-relative include globs using '/' separators.
    exclude:
        Root-relative exclude globs using '/' separators.

    Notes
    -----
    Template is the authoritative owner of these fields. This type remains on
    the Job/ProfileStore path only as a compatibility carrier until a later
    boundary move relocates policy ownership more fully.
    """

    include: tuple[str, ...]
    exclude: tuple[str, ...]

    def to_template_selection_rules(self) -> TemplateSelectionRules:
        """
        Return the Template-owned view of these selection rules.

        Returns
        -------
        TemplateSelectionRules
            Template-authoritative selection rules represented by this
            compatibility carrier.
        """

        return TemplateSelectionRules(include=self.include, exclude=self.exclude)


@dataclass(frozen=True, slots=True)
class JobBackupDefaults:
    """
    Transitional mixed-ownership carrier for current job backup inputs.

    Attributes
    ----------
    source_root:
        Job-owned live target binding for the current backup definition.
    compression:
        Template-owned compression mode carried here for compatibility only.

    Notes
    -----
    This type is not an authoritative policy owner. ``source_root`` remains
    Job-owned target binding data, while ``compression`` is Template-owned
    policy mirrored here as transitional compatibility state. Scheduling does
    not own either field.
    """

    source_root: str
    compression: str

    def to_template_policy(self, rules: RuleSet) -> TemplatePolicy:
        """
        Return the Template-owned policy represented by compatibility carriers.

        Parameters
        ----------
        rules:
            Transitional carrier for Template-owned selection rules.

        Returns
        -------
        TemplatePolicy
            Template-authoritative policy view derived from compatibility data.

        Notes
        -----
        ``source_root`` is intentionally excluded because it is Job-owned target
        binding, not Template policy.
        """

        return TemplatePolicy(
            selection_rules=rules.to_template_selection_rules(),
            compression=self.compression,
        )


class ProfileStore(Protocol):
    """
    Persistence API for current job authoring and transitional policy carriers.

    Implementations are engine-owned. The GUI should interact through this
    interface (typically via a GUI adapter that runs calls off the UI thread).

    Notes
    -----
    Template policy is authoritative for selection rules and compression. This
    protocol still exposes Job-shaped compatibility carriers because runtime and
    persistence relocation are deferred to later boundary corrections.
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

    def load_rules(self, job_id: JobId) -> RuleSet:
        """
        Load transitional Job-shaped copies of Template-owned selection rules.

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
        Persist transitional Job-shaped copies of Template-owned selection rules.

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

        Notes
        -----
        The current routing key is still discovered from the Job path during the
        transition, but the returned selection rules are Template-owned policy,
        not Job-owned meaning.
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

        Notes
        -----
        Implementations may continue updating Job-shaped compatibility mirrors,
        but those mirrors must not become co-equal owners.
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

        Notes
        -----
        The current routing key is still discovered from the Job path during the
        transition, but the returned compression value is Template-owned policy,
        not Job-owned meaning.
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

        Notes
        -----
        Implementations may continue updating Job-shaped compatibility mirrors,
        but those mirrors must not become co-equal owners.
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
            Persisted scheduled-task record. Trigger metadata is authoritative
            for scheduling; any attached backup-definition data is transitional
            compatibility state only.

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
            Scheduled-task record to persist. Scheduling owns trigger metadata
            only; any attached backup-definition payload is transitional
            compatibility state.

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

    def load_job_backup_defaults(self, job_id: JobId) -> JobBackupDefaults:
        """
        Load the current Job-shaped compatibility carrier for backup inputs.

        Parameters
        ----------
        job_id:
            Identifier of the job to load.

        Returns
        -------
        JobBackupDefaults
            Compatibility carrier containing Job-owned target binding plus any
            mirrored Template-owned compression policy.

        Raises
        ------
        UnknownJobError
            If the job or defaults are not present.
        """
        raise NotImplementedError

    def save_job_backup_defaults(self, job_id: JobId, defaults: JobBackupDefaults) -> None:
        """
        Persist the current Job-shaped compatibility carrier for backup inputs.

        Parameters
        ----------
        job_id:
            Identifier of the job to update.
        defaults:
            Compatibility carrier containing Job-owned target binding plus any
            mirrored Template-owned compression policy.

        Raises
        ------
        UnknownJobError
            If the target job does not exist.
        """
        raise NotImplementedError
