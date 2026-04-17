"""High-level scheduling orchestration for WCBT backup jobs.

Notes
-----
Scheduling is a trigger-only support layer.
"""

from __future__ import annotations

from pathlib import Path

from backup_engine.errors import SchedulingBackendError
from backup_engine.job_binding import JobBinding
from backup_engine.paths_and_safety import resolve_profile_paths
from backup_engine.profile_store.api import ProfileStore
from backup_engine.profile_store.sqlite_store import open_profile_store

from .models import BackupScheduleSpec, ScheduledBackupStatus, normalize_schedule_spec
from .schtasks_backend import SchtasksBackend
from .wrapper_scripts import (
    build_schtasks_wrapper_command,
    delete_scheduled_job_wrapper,
    scheduled_wrapper_paths,
    write_scheduled_job_wrapper,
)

SCHEDULED_BACKUP_NOTE = "Scheduled backup executed by Windows Task Scheduler"


def scheduled_task_name(*, profile_name: str, job_id: str) -> str:
    """
    Derive the stable Windows task name for a scheduled WCBT backup.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    job_id:
        Stable WCBT job identifier.

    Returns
    -------
    str
        Stable Task Scheduler task name.
    """
    return f"WCBT-{profile_name.strip()}-{job_id.strip()}"


def create_or_update_scheduled_backup(
    *,
    profile_name: str,
    data_root: Path | None,
    schedule: BackupScheduleSpec,
    backend: SchtasksBackend | None = None,
) -> ScheduledBackupStatus:
    """
    Persist and register a scheduled backup trigger for a WCBT job.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    data_root:
        Optional WCBT data root override.
    schedule:
        Schedule definition to persist and register.
    backend:
        Optional backend override for tests.

    Returns
    -------
    ScheduledBackupStatus
        Combined persisted and Windows scheduler state after creation.

    Notes
    -----
    The scheduling boundary owns trigger metadata only. Any backup-definition
    data attached to the persisted record remains transitional compatibility
    state for the current scheduled execution path.
    """
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    normalized = _resolve_schedule_against_current_job_state(store=store, schedule=schedule)
    task_name = scheduled_task_name(profile_name=profile_name, job_id=normalized.job_id)
    wrapper_path = write_scheduled_job_wrapper(
        profile_name=profile_name,
        data_root=data_root,
        job_id=normalized.job_id,
    )
    if not wrapper_path.is_file():
        raise SchedulingBackendError(f"Scheduled wrapper was not created at: {wrapper_path}")

    store.save_backup_schedule(normalized)
    (backend or SchtasksBackend()).create_task(
        task_name=task_name,
        task_command=build_schtasks_wrapper_command(wrapper_path),
        cadence=normalized.cadence,
        start_time_local=normalized.start_time_local,
        weekdays=normalized.weekdays,
        interval_unit=normalized.interval_unit,
        interval_value=normalized.interval_value,
    )
    status = query_scheduled_backup(
        profile_name=profile_name,
        data_root=data_root,
        job_id=normalized.job_id,
        backend=backend,
    )
    _verify_scheduled_backup_creation(status)
    return status


def query_scheduled_backup(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
    backend: SchtasksBackend | None = None,
) -> ScheduledBackupStatus:
    """
    Load persisted scheduled-task data and current Windows task presence.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.
    backend:
        Optional backend override for tests.

    Returns
    -------
    ScheduledBackupStatus
        Persisted scheduled-task trigger plus current Windows task presence
        details, with current authoritative Job and Template data attached for
        user-facing convenience.
    """
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    schedule = store.load_backup_schedule(job_id)
    current_job_binding = store.load_job_binding(job_id)
    current_template_compression = store.load_template_compression(current_job_binding.job_id)
    task_name = scheduled_task_name(profile_name=profile_name, job_id=job_id)
    task_info = (backend or SchtasksBackend()).query_task(task_name=task_name)
    task_enabled = _parse_task_enabled(task_info.details)
    wrapper_path = scheduled_wrapper_paths(
        profile_name=profile_name,
        data_root=data_root,
        job_id=job_id,
    ).wrapper_path
    return ScheduledBackupStatus(
        schedule=schedule,
        current_job_binding=current_job_binding,
        current_template_compression=current_template_compression,
        task_name=task_name,
        wrapper_path=str(wrapper_path),
        wrapper_exists=wrapper_path.is_file(),
        task_exists=task_info.exists,
        task_enabled=task_enabled,
        scheduler_details=dict(task_info.details),
    )


def delete_scheduled_backup(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
    backend: SchtasksBackend | None = None,
) -> None:
    """
    Remove a scheduled backup from both WCBT persistence and Windows.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.
    backend:
        Optional backend override for tests.
    """
    task_name = scheduled_task_name(profile_name=profile_name, job_id=job_id)
    (backend or SchtasksBackend()).delete_task(task_name=task_name)
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    store.delete_backup_schedule(job_id)
    delete_scheduled_job_wrapper(profile_name=profile_name, data_root=data_root, job_id=job_id)


def run_scheduled_backup_now(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
    backend: SchtasksBackend | None = None,
) -> None:
    """
    Start an existing scheduled backup task immediately.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.
    backend:
        Optional backend override for tests.
    """
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    store.load_backup_schedule(job_id)
    task_name = scheduled_task_name(profile_name=profile_name, job_id=job_id)
    (backend or SchtasksBackend()).run_task(task_name=task_name)


def set_scheduled_backup_enabled(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
    enabled: bool,
    backend: SchtasksBackend | None = None,
) -> ScheduledBackupStatus:
    """
    Enable or disable an existing scheduled backup task.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.
    enabled:
        Desired enabled state for the backing Windows task.
    backend:
        Optional backend override for tests.

    Returns
    -------
    ScheduledBackupStatus
        Updated persisted trigger plus current Windows task state.
    """
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    store.load_backup_schedule(job_id)
    task_name = scheduled_task_name(profile_name=profile_name, job_id=job_id)
    (backend or SchtasksBackend()).set_task_enabled(task_name=task_name, enabled=enabled)
    return query_scheduled_backup(
        profile_name=profile_name,
        data_root=data_root,
        job_id=job_id,
        backend=backend,
    )


def load_scheduled_backup_run_request(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
) -> tuple[JobBinding, str]:
    """
    Load the current Job binding and Template compression for a scheduled run.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.

    Returns
    -------
    tuple[JobBinding, str]
        Current authoritative Job binding and current Template compression.
    """
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    current_job_binding = store.load_job_binding(job_id)
    compression = store.load_template_compression(current_job_binding.job_id)
    return current_job_binding, compression


def _resolve_schedule_against_current_job_state(
    *,
    store: ProfileStore,
    schedule: BackupScheduleSpec,
) -> BackupScheduleSpec:
    """
    Rebuild a schedule so trigger data is saved without mutating Job/Template state.

    Parameters
    ----------
    store:
        Open profile store used to read authoritative Job and Template state.
    schedule:
        Candidate scheduling payload from the caller.

    Returns
    -------
    BackupScheduleSpec
        Normalized schedule populated with current authoritative compatibility
        fields for the existing execution path.
    """
    normalized = normalize_schedule_spec(schedule)
    current_job_binding = store.load_job_binding(normalized.job_id)
    current_template_compression = store.load_template_compression(normalized.job_id)
    return BackupScheduleSpec(
        job_id=normalized.job_id,
        source_root=current_job_binding.source_root,
        cadence=normalized.cadence,
        start_time_local=normalized.start_time_local,
        weekdays=normalized.weekdays,
        compression=current_template_compression,
        interval_unit=normalized.interval_unit,
        interval_value=normalized.interval_value,
    )


def run_scheduled_job(
    *,
    profile_name: str,
    job_id: str,
    data_root: Path | None,
    mode: str,
) -> None:
    """
    Execute a scheduled job through the standard backup orchestration path.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the job.
    job_id:
        Stable WCBT job identifier to execute.
    data_root:
        Optional WCBT data root override.
    mode:
        Scheduled execution mode.

    Raises
    ------
    ValueError
        If the requested mode is not supported.
    """
    if mode != "execute-compress":
        raise ValueError(f"Unsupported scheduled run mode: {mode!r}")

    job_binding, compression = load_scheduled_backup_run_request(
        profile_name=profile_name,
        data_root=data_root,
        job_id=job_id,
    )
    run_backup_data_root = resolve_profile_paths(
        profile_name=profile_name,
        data_root=data_root,
    ).data_root
    from backup_engine.backup.service import run_backup

    run_backup(
        profile_name=profile_name,
        source=Path(job_binding.source_root),
        dry_run=False,
        data_root=run_backup_data_root,
        backup_origin="scheduled",
        backup_note=SCHEDULED_BACKUP_NOTE,
        execute=True,
        compress=True,
        compression=compression,
        job_id=job_id,
        job_name=job_binding.job_name,
    )


def _parse_task_enabled(details: dict[str, str] | object) -> bool | None:
    """
    Infer task enabled state from verbose ``schtasks`` detail fields.

    Parameters
    ----------
    details:
        Scheduler detail mapping returned by ``schtasks /query``.

    Returns
    -------
    bool | None
        Enabled state when it can be inferred, otherwise ``None``.
    """
    if not isinstance(details, dict):
        return None

    for key in ("Scheduled Task State", "Status"):
        raw_value = details.get(key)
        if raw_value is None:
            continue
        normalized = raw_value.strip().lower()
        if not normalized:
            continue
        if "disabled" in normalized:
            return False
        if any(token in normalized for token in ("ready", "running", "queued")):
            return True
    return None


def _verify_scheduled_backup_creation(status: ScheduledBackupStatus) -> None:
    """
    Validate that a saved schedule is backed by real wrapper and task artifacts.

    Parameters
    ----------
    status:
        Combined post-save WCBT and Task Scheduler view of the scheduled job.

    Raises
    ------
    SchedulingBackendError
        If wrapper generation or Task Scheduler registration cannot be verified.
    """
    if not status.wrapper_exists:
        raise SchedulingBackendError(
            f"Scheduled wrapper is missing after save: {status.wrapper_path}"
        )
    if not status.task_exists:
        raise SchedulingBackendError(
            "Task Scheduler did not confirm the scheduled task after save. "
            f"Expected task name: {status.task_name}"
        )
