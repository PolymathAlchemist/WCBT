"""Typed models for WCBT Windows scheduling.

Notes
-----
Scheduling is a trigger-only concern in the WCBT domain model.
Any backup-definition data still attached to scheduled execution is transitional
compatibility state, not scheduler-owned meaning.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from backup_engine.errors import InvalidScheduleError

if TYPE_CHECKING:
    from backup_engine.job_binding import JobBinding

_WEEKDAY_ORDER: tuple[str, ...] = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
_WEEKDAY_INDEX: dict[str, int] = {day: idx for idx, day in enumerate(_WEEKDAY_ORDER)}


@dataclass(frozen=True, slots=True)
class ScheduledTaskTriggerSpec:
    """
    Trigger-only metadata for a scheduled WCBT job invocation.

    Attributes
    ----------
    job_id:
        Stable WCBT job identifier to invoke.
    cadence:
        Supported cadence kind. v1 supports only ``daily`` and ``weekly``.
    start_time_local:
        Start time in local 24-hour ``HH:MM`` format.
    weekdays:
        Weekly day set using Task Scheduler day tokens such as ``MON``.
        Empty for daily schedules.
    interval_unit:
        Interval unit for ``interval`` schedules. Supported values are
        ``"minutes"`` and ``"hours"``.
    interval_value:
        Repeat interval magnitude for ``interval`` schedules.
    """

    job_id: str
    cadence: str
    start_time_local: str
    weekdays: tuple[str, ...]
    interval_unit: str | None = None
    interval_value: int | None = None


@dataclass(frozen=True, slots=True)
class ScheduledBackupLegacyDefinition:
    """
    Transitional scheduled-backup inputs carried for compatibility only.

    Notes
    -----
    These fields are not owned by the scheduling boundary. They exist only
    because the current scheduled execution path still reads backup-definition
    inputs from persisted schedule state until later boundary corrections remove
    that dependency. ``source_root`` remains Job-owned target binding, while
    ``compression`` remains Template-owned policy carried here only for
    compatibility.
    """

    source_root: str
    compression: str


@dataclass(frozen=True, slots=True, init=False)
class BackupScheduleSpec:
    """
    Persisted scheduled-task record for a backup job.

    Attributes
    ----------
    trigger:
        Trigger-only scheduling metadata.
    legacy_definition:
        Transitional compatibility payload for the current scheduled execution
        path. This is not scheduler-owned meaning.

    Notes
    -----
    This type intentionally separates trigger metadata from any compatibility
    backup-definition inputs so scheduling cannot be modeled as the owner of
    backup content semantics.
    """

    trigger: ScheduledTaskTriggerSpec
    legacy_definition: ScheduledBackupLegacyDefinition | None

    def __init__(
        self,
        *,
        job_id: str,
        source_root: str,
        cadence: str,
        start_time_local: str,
        weekdays: tuple[str, ...],
        compression: str,
        interval_unit: str | None = None,
        interval_value: int | None = None,
    ) -> None:
        object.__setattr__(
            self,
            "trigger",
            ScheduledTaskTriggerSpec(
                job_id=job_id,
                cadence=cadence,
                start_time_local=start_time_local,
                weekdays=weekdays,
                interval_unit=interval_unit,
                interval_value=interval_value,
            ),
        )
        object.__setattr__(
            self,
            "legacy_definition",
            ScheduledBackupLegacyDefinition(
                source_root=source_root,
                compression=compression,
            ),
        )

    @classmethod
    def from_parts(
        cls,
        *,
        trigger: ScheduledTaskTriggerSpec,
        legacy_definition: ScheduledBackupLegacyDefinition | None,
    ) -> "BackupScheduleSpec":
        """
        Build a schedule record from explicitly separated parts.

        Parameters
        ----------
        trigger:
            Trigger-only scheduling metadata.
        legacy_definition:
            Transitional compatibility payload for scheduled execution.

        Returns
        -------
        BackupScheduleSpec
            Persisted scheduled-task record with separated ownership.
        """
        schedule = cls.__new__(cls)
        object.__setattr__(schedule, "trigger", trigger)
        object.__setattr__(schedule, "legacy_definition", legacy_definition)
        return schedule

    @property
    def job_id(self) -> str:
        """Return the job identifier from trigger metadata."""
        return self.trigger.job_id

    @property
    def cadence(self) -> str:
        """Return the scheduled cadence from trigger metadata."""
        return self.trigger.cadence

    @property
    def start_time_local(self) -> str:
        """Return the scheduled local start time from trigger metadata."""
        return self.trigger.start_time_local

    @property
    def weekdays(self) -> tuple[str, ...]:
        """Return the scheduled weekdays from trigger metadata."""
        return self.trigger.weekdays

    @property
    def source_root(self) -> str:
        """
        Return transitional scheduled execution source input.

        Notes
        -----
        This property exists for compatibility with the current scheduled-run
        path. It does not imply scheduler ownership of backup-definition data.
        """
        if self.legacy_definition is None:
            raise InvalidScheduleError("Scheduled backup source_root is not available.")
        return self.legacy_definition.source_root

    @property
    def interval_unit(self) -> str | None:
        """Return the interval unit from trigger metadata."""
        return self.trigger.interval_unit

    @property
    def interval_value(self) -> int | None:
        """Return the interval magnitude from trigger metadata."""
        return self.trigger.interval_value

    @property
    def compression(self) -> str:
        """
        Return transitional scheduled execution compression input.

        Notes
        -----
        This property exists for compatibility with the current scheduled-run
        path. It does not imply scheduler ownership of backup-definition data.
        """
        if self.legacy_definition is None:
            raise InvalidScheduleError("Scheduled backup compression is not available.")
        return self.legacy_definition.compression


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
        Persisted scheduled-task record. Trigger metadata is authoritative for
        this boundary; any attached legacy definition payload is transitional
        compatibility state only.
    current_job_binding:
        Current authoritative Job binding. This is the canonical Job read shape
        for identity, Template reference, and live target binding.
    current_template_compression:
        Current authoritative Template compression policy shown alongside the
        trigger for user-facing convenience.
    task_name:
        Derived Windows task name for the job.
    wrapper_path:
        Generated per-job wrapper path registered with Task Scheduler.
    wrapper_exists:
        Whether the generated wrapper file currently exists on disk.
    task_exists:
        Whether the backing Windows task currently exists.
    task_enabled:
        Whether the backing task currently appears enabled. ``None`` means WCBT
        could not determine the state from available scheduler details.
    scheduler_details:
        Verbose ``schtasks`` fields when they are available.
    """

    schedule: BackupScheduleSpec
    current_job_binding: JobBinding
    current_template_compression: str | None
    task_name: str
    wrapper_path: str
    wrapper_exists: bool
    task_exists: bool
    task_enabled: bool | None
    scheduler_details: Mapping[str, str]


def normalize_schedule_spec(spec: BackupScheduleSpec) -> BackupScheduleSpec:
    """
    Validate and normalize a persisted scheduled-task record.

    Parameters
    ----------
    spec:
        Candidate scheduled-task record.

    Returns
    -------
    BackupScheduleSpec
        Normalized scheduled-task record suitable for persistence and task creation.

    Raises
    ------
    InvalidScheduleError
        If the schedule falls outside the deliberately small v1 surface.

    Notes
    -----
    This normalization is intentionally restrictive so the first Task Scheduler
    milestone stays low-risk and maps directly to ``schtasks`` switches.
    """
    trigger = spec.trigger
    job_id = trigger.job_id.strip()
    if not job_id:
        raise InvalidScheduleError("job_id must not be empty.")

    legacy_definition = spec.legacy_definition
    if legacy_definition is None:
        raise InvalidScheduleError("legacy scheduled execution inputs must be present.")

    source_root = legacy_definition.source_root.strip()
    if not source_root:
        raise InvalidScheduleError("source_root must not be empty.")

    cadence = trigger.cadence.strip().lower()
    if cadence not in {"daily", "weekly", "interval"}:
        raise InvalidScheduleError("cadence must be 'daily', 'weekly', or 'interval'.")

    start_time_local = normalize_start_time_local(trigger.start_time_local)

    compression = legacy_definition.compression.strip().lower()
    if compression not in {"none", "zip", "tar.zst"}:
        raise InvalidScheduleError("compression must be one of: none, zip, tar.zst.")

    weekdays = normalize_weekdays(trigger.weekdays)
    if cadence == "daily" and weekdays:
        raise InvalidScheduleError("daily schedules must not specify weekdays.")
    if cadence == "weekly" and not weekdays:
        raise InvalidScheduleError("weekly schedules must specify at least one weekday.")
    if cadence == "interval" and weekdays:
        raise InvalidScheduleError("interval schedules must not specify weekdays.")

    interval_unit = normalize_interval_unit(trigger.interval_unit)
    interval_value = normalize_interval_value(trigger.interval_value)
    if cadence == "interval":
        if interval_unit is None or interval_value is None:
            raise InvalidScheduleError(
                "interval schedules must specify interval_unit and interval_value."
            )
    else:
        if interval_unit is not None or interval_value is not None:
            raise InvalidScheduleError(
                "daily/weekly schedules must not specify interval_unit or interval_value."
            )

    return BackupScheduleSpec.from_parts(
        trigger=ScheduledTaskTriggerSpec(
            job_id=job_id,
            cadence=cadence,
            start_time_local=start_time_local,
            weekdays=weekdays,
            interval_unit=interval_unit,
            interval_value=interval_value,
        ),
        legacy_definition=ScheduledBackupLegacyDefinition(
            source_root=source_root,
            compression=compression,
        ),
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


def normalize_interval_unit(value: str | None) -> str | None:
    """
    Normalize an interval cadence unit.

    Parameters
    ----------
    value:
        Candidate interval unit.

    Returns
    -------
    str | None
        Normalized interval unit, or ``None`` when no unit is supplied.

    Raises
    ------
    InvalidScheduleError
        If the unit is outside the supported interval cadence surface.
    """
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized not in {"minutes", "hours"}:
        raise InvalidScheduleError("interval_unit must be 'minutes' or 'hours'.")
    return normalized


def normalize_interval_value(value: int | None) -> int | None:
    """
    Normalize an interval cadence magnitude.

    Parameters
    ----------
    value:
        Candidate interval magnitude.

    Returns
    -------
    int | None
        Normalized interval magnitude, or ``None`` when not supplied.

    Raises
    ------
    InvalidScheduleError
        If the magnitude is outside the supported interval cadence surface.
    """
    if value is None:
        return None
    if value <= 0:
        raise InvalidScheduleError("interval_value must be greater than 0.")
    return value
