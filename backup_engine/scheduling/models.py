"""Typed models for WCBT Windows scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from backup_engine.errors import InvalidScheduleError

_WEEKDAY_ORDER: tuple[str, ...] = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
_WEEKDAY_INDEX: dict[str, int] = {day: idx for idx, day in enumerate(_WEEKDAY_ORDER)}


@dataclass(frozen=True, slots=True)
class BackupScheduleSpec:
    """
    Minimal persisted schedule definition for a backup job.

    Attributes
    ----------
    job_id:
        Stable WCBT job identifier.
    source_root:
        Source directory that scheduled runs back up.
    cadence:
        Supported cadence kind. v1 supports only ``daily`` and ``weekly``.
    start_time_local:
        Start time in local 24-hour ``HH:MM`` format.
    weekdays:
        Weekly day set using Task Scheduler day tokens such as ``MON``.
        Empty for daily schedules.
    compression:
        Compression behavior for scheduled execute-mode backups.
    """

    job_id: str
    source_root: str
    cadence: str
    start_time_local: str
    weekdays: tuple[str, ...]
    compression: str


@dataclass(frozen=True, slots=True)
class ScheduledTaskInfo:
    """
    Result of querying a single Windows scheduled task.

    Attributes
    ----------
    task_name:
        Task Scheduler task name used by WCBT.
    exists:
        Whether the task exists according to ``schtasks /query``.
    details:
        Parsed ``key: value`` fields from verbose query output when available.
    raw_output:
        Unparsed stdout from ``schtasks /query`` for diagnostics.
    """

    task_name: str
    exists: bool
    details: Mapping[str, str]
    raw_output: str | None


@dataclass(frozen=True, slots=True)
class ScheduledBackupStatus:
    """
    Combined WCBT and Windows scheduler view of one scheduled backup.

    Attributes
    ----------
    schedule:
        Persisted WCBT schedule definition.
    task_name:
        Derived Windows task name for the job.
    task_exists:
        Whether the backing Windows task currently exists.
    scheduler_details:
        Verbose ``schtasks`` fields when they are available.
    """

    schedule: BackupScheduleSpec
    task_name: str
    task_exists: bool
    scheduler_details: Mapping[str, str]


def normalize_schedule_spec(spec: BackupScheduleSpec) -> BackupScheduleSpec:
    """
    Validate and normalize a backup schedule specification.

    Parameters
    ----------
    spec:
        Candidate schedule definition.

    Returns
    -------
    BackupScheduleSpec
        Normalized schedule definition suitable for persistence and task creation.

    Raises
    ------
    InvalidScheduleError
        If the schedule falls outside the deliberately small v1 surface.

    Notes
    -----
    This normalization is intentionally restrictive so the first Task Scheduler
    milestone stays low-risk and maps directly to ``schtasks`` switches.
    """
    job_id = spec.job_id.strip()
    if not job_id:
        raise InvalidScheduleError("job_id must not be empty.")

    source_root = spec.source_root.strip()
    if not source_root:
        raise InvalidScheduleError("source_root must not be empty.")

    cadence = spec.cadence.strip().lower()
    if cadence not in {"daily", "weekly"}:
        raise InvalidScheduleError("cadence must be 'daily' or 'weekly'.")

    start_time_local = normalize_start_time_local(spec.start_time_local)

    compression = spec.compression.strip().lower()
    if compression not in {"none", "zip", "tar.zst"}:
        raise InvalidScheduleError("compression must be one of: none, zip, tar.zst.")

    weekdays = normalize_weekdays(spec.weekdays)
    if cadence == "daily" and weekdays:
        raise InvalidScheduleError("daily schedules must not specify weekdays.")
    if cadence == "weekly" and not weekdays:
        raise InvalidScheduleError("weekly schedules must specify at least one weekday.")

    return BackupScheduleSpec(
        job_id=job_id,
        source_root=source_root,
        cadence=cadence,
        start_time_local=start_time_local,
        weekdays=weekdays,
        compression=compression,
    )


def normalize_start_time_local(value: str) -> str:
    """
    Normalize a local schedule time into Task Scheduler ``HH:MM`` form.

    Parameters
    ----------
    value:
        Candidate local time string.

    Returns
    -------
    str
        Normalized 24-hour time string.

    Raises
    ------
    InvalidScheduleError
        If the value is not a valid local ``HH:MM`` time.
    """
    candidate = value.strip()
    try:
        parsed = datetime.strptime(candidate, "%H:%M")
    except ValueError as exc:
        raise InvalidScheduleError("start_time_local must use 24-hour HH:MM format.") from exc
    return parsed.strftime("%H:%M")


def normalize_weekdays(values: Sequence[str] | Iterable[str]) -> tuple[str, ...]:
    """
    Normalize weekday tokens into stable Task Scheduler ordering.

    Parameters
    ----------
    values:
        Candidate weekday tokens such as ``MON`` or ``fri``.

    Returns
    -------
    tuple[str, ...]
        Unique weekday tokens ordered Monday through Sunday.

    Raises
    ------
    InvalidScheduleError
        If any token is not supported by the first-pass scheduler surface.
    """
    normalized: set[str] = set()
    for raw in values:
        day = str(raw).strip().upper()
        if not day:
            continue
        if day not in _WEEKDAY_INDEX:
            raise InvalidScheduleError(f"Unsupported weekday token: {raw!r}")
        normalized.add(day)
    return tuple(sorted(normalized, key=_WEEKDAY_INDEX.__getitem__))
