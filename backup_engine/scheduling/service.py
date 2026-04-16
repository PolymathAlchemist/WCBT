"""High-level scheduling orchestration for WCBT backup jobs.

Notes
-----
Scheduling is a trigger-only support layer.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.sqlite_store import open_profile_store

from .models import BackupScheduleSpec, ScheduledBackupStatus, normalize_schedule_spec
from .schtasks_backend import SchtasksBackend


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
    python_executable: str | None = None,
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
    python_executable:
        Optional interpreter path override for tests.

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
    normalized = normalize_schedule_spec(schedule)
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    task_name = scheduled_task_name(profile_name=profile_name, job_id=normalized.job_id)
    task_command = _build_scheduled_backup_command(
        profile_name=profile_name,
        job_id=normalized.job_id,
        data_root=data_root,
        python_executable=python_executable,
    )

    store.save_backup_schedule(normalized)
    (backend or SchtasksBackend()).create_task(
        task_name=task_name,
        task_command=task_command,
        cadence=normalized.cadence,
        start_time_local=normalized.start_time_local,
        weekdays=normalized.weekdays,
    )
    return query_scheduled_backup(
        profile_name=profile_name,
        data_root=data_root,
        job_id=normalized.job_id,
        backend=backend,
    )


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
    return ScheduledBackupStatus(
        schedule=schedule,
        current_job_binding=current_job_binding,
        current_template_compression=current_template_compression,
        task_name=task_name,
        task_exists=task_info.exists,
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


def _build_scheduled_backup_command(
    *,
    profile_name: str,
    job_id: str,
    data_root: Path | None,
    python_executable: str | None,
) -> str:
    """
    Build the command line registered with Windows Task Scheduler.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the task.
    job_id:
        Stable WCBT job identifier.
    data_root:
        Optional WCBT data root override.
    python_executable:
        Optional interpreter path override for tests.

    Returns
    -------
    str
        Quoted Windows command line for ``schtasks /tr``.

    Notes
    -----
    The command is intentionally routed through ``python -m wcbt`` so the Task
    Scheduler action stays thin and WCBT retains ownership of backup orchestration.
    """
    command = [
        python_executable or sys.executable,
        "-m",
        "wcbt",
        "scheduled-backup",
        "--profile",
        profile_name,
        "--job-id",
        job_id,
    ]
    if data_root is not None:
        command.extend(["--data-root", str(data_root)])
    return subprocess.list2cmdline(command)
