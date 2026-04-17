"""Thin subprocess adapter for ``schtasks.exe``."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Callable

from backup_engine.errors import ScheduledTaskNotFoundError, SchedulingBackendError

from .models import ScheduledTaskInfo

CompletedProcessRunner = Callable[..., subprocess.CompletedProcess[str]]


@dataclass(slots=True)
class SchtasksBackend:
    """
    Minimal backend that shells out to ``schtasks.exe``.

    Parameters
    ----------
    runner:
        Injectable subprocess runner used for testing.

    Notes
    -----
    The backend is intentionally small and command-oriented so higher-level WCBT
    services can remain ignorant of ``subprocess`` details and Windows-specific
    output handling.
    """

    runner: CompletedProcessRunner = subprocess.run

    def create_task(
        self,
        *,
        task_name: str,
        task_command: str,
        cadence: str,
        start_time_local: str,
        weekdays: Sequence[str],
        interval_unit: str | None = None,
        interval_value: int | None = None,
    ) -> None:
        """
        Create or replace a Windows scheduled task.

        Parameters
        ----------
        task_name:
            Windows task name.
        task_command:
            Full command line passed to ``/tr``.
        cadence:
            Either ``daily`` or ``weekly``.
        start_time_local:
            Local start time in ``HH:MM`` format.
        weekdays:
            Weekly day tokens for ``weekly`` schedules.
        interval_unit:
            Interval unit for ``interval`` schedules.
        interval_value:
            Interval magnitude for ``interval`` schedules.
        """
        schedule_cadence = cadence.upper()
        modifier_args: list[str] = []
        if cadence == "interval":
            if interval_unit == "minutes":
                schedule_cadence = "MINUTE"
            elif interval_unit == "hours":
                schedule_cadence = "HOURLY"
            else:
                raise SchedulingBackendError(
                    f"Unsupported interval unit for schtasks: {interval_unit!r}"
                )
            if interval_value is None:
                raise SchedulingBackendError("interval_value is required for interval schedules.")
            modifier_args = ["/mo", str(interval_value)]

        args = [
            "/create",
            "/f",
            "/tn",
            task_name,
            "/tr",
            task_command,
            "/sc",
            schedule_cadence,
            *modifier_args,
            "/st",
            start_time_local,
            "/it",
            "/rl",
            "LIMITED",
        ]
        if cadence == "weekly":
            args.extend(["/d", ",".join(weekdays)])
        self._run(args)

    def query_task(self, *, task_name: str) -> ScheduledTaskInfo:
        """
        Query a Windows scheduled task by name.

        Parameters
        ----------
        task_name:
            Windows task name.

        Returns
        -------
        ScheduledTaskInfo
            Presence flag plus parsed verbose output when available.
        """
        try:
            completed = self._run(["/query", "/tn", task_name, "/fo", "LIST", "/v"])
        except ScheduledTaskNotFoundError:
            return ScheduledTaskInfo(
                task_name=task_name,
                exists=False,
                details={},
                raw_output=None,
            )

        return ScheduledTaskInfo(
            task_name=task_name,
            exists=True,
            details=_parse_list_output(completed.stdout),
            raw_output=completed.stdout,
        )

    def delete_task(self, *, task_name: str) -> None:
        """
        Delete a Windows scheduled task if it exists.

        Parameters
        ----------
        task_name:
            Windows task name.

        Notes
        -----
        Missing tasks are treated as already-deleted so WCBT cleanup remains
        idempotent.
        """
        try:
            self._run(["/delete", "/tn", task_name, "/f"])
        except ScheduledTaskNotFoundError:
            return

    def run_task(self, *, task_name: str) -> None:
        """
        Start a Windows scheduled task immediately.

        Parameters
        ----------
        task_name:
            Windows task name.
        """
        self._run(["/run", "/tn", task_name])

    def set_task_enabled(self, *, task_name: str, enabled: bool) -> None:
        """
        Enable or disable a Windows scheduled task.

        Parameters
        ----------
        task_name:
            Windows task name.
        enabled:
            Desired enabled state.
        """
        self._run(["/change", "/tn", task_name, "/enable" if enabled else "/disable"])

    def _run(self, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        """
        Execute ``schtasks.exe`` and translate failures into WCBT errors.

        Parameters
        ----------
        args:
            Arguments following the ``schtasks`` executable.

        Returns
        -------
        subprocess.CompletedProcess[str]
            Completed process object for successful execution.

        Raises
        ------
        ScheduledTaskNotFoundError
            If the command indicates a missing task.
        SchedulingBackendError
            If the command fails for any other reason.
        """
        command = ["schtasks", *args]
        completed = self.runner(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            return completed

        text = "\n".join(part for part in (completed.stdout, completed.stderr) if part).strip()
        if _looks_like_missing_task(text):
            raise ScheduledTaskNotFoundError(text or "Scheduled task not found.")
        raise SchedulingBackendError(
            (
                f"schtasks command failed: {subprocess.list2cmdline(command)}\n"
                f"return code: {completed.returncode}\n"
                f"stdout: {completed.stdout.strip()}\n"
                f"stderr: {completed.stderr.strip()}"
            ).strip()
        )


def _parse_list_output(raw_output: str) -> dict[str, str]:
    """
    Parse ``schtasks /query /fo LIST /v`` output into key-value pairs.

    Parameters
    ----------
    raw_output:
        Raw stdout from ``schtasks``.

    Returns
    -------
    dict[str, str]
        Parsed key-value pairs. Unparseable lines are ignored.

    Notes
    -----
    The keys are preserved exactly as emitted by Windows. WCBT treats them as
    display-oriented diagnostics instead of locale-stable API fields.
    """
    details: dict[str, str] = {}
    for line in raw_output.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if not key:
            continue
        details[key] = value.strip()
    return details


def _looks_like_missing_task(text: str) -> bool:
    """
    Heuristically detect ``schtasks`` messages for missing tasks.

    Parameters
    ----------
    text:
        Combined stdout/stderr text from ``schtasks``.

    Returns
    -------
    bool
        True when the message strongly suggests the task does not exist.

    Notes
    -----
    ``schtasks`` does not expose structured machine-readable error codes through
    this subprocess interface, so the first-pass adapter relies on conservative
    text matching for the common missing-task cases.
    """
    lowered = text.lower()
    return (
        "cannot find" in lowered
        or "does not exist" in lowered
        or "the system cannot find the file specified" in lowered
        or "the system cannot find the path specified" in lowered
    )
