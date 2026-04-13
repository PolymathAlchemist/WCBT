from __future__ import annotations

import subprocess
from collections.abc import Iterable

import pytest

from backup_engine.errors import ScheduledTaskNotFoundError, SchedulingBackendError
from backup_engine.scheduling.schtasks_backend import SchtasksBackend


def _completed(
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    """
    Build a completed-process object for backend tests.

    Parameters
    ----------
    returncode:
        Process exit code.
    stdout:
        Captured stdout text.
    stderr:
        Captured stderr text.

    Returns
    -------
    subprocess.CompletedProcess[str]
        Synthetic subprocess result.
    """
    return subprocess.CompletedProcess(
        args=["schtasks"], returncode=returncode, stdout=stdout, stderr=stderr
    )


def test_create_task_builds_daily_command() -> None:
    seen: list[list[str]] = []

    def _runner(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        command = args[0]
        assert isinstance(command, Iterable)
        seen.append([str(part) for part in command])
        return _completed(stdout="SUCCESS")

    backend = SchtasksBackend(runner=_runner)
    backend.create_task(
        task_name="WCBT-default-job1",
        task_command='"C:\\Python\\python.exe" -m wcbt scheduled-backup --profile default --job-id job1',
        cadence="daily",
        start_time_local="06:30",
        weekdays=(),
    )

    assert seen == [
        [
            "schtasks",
            "/create",
            "/f",
            "/tn",
            "WCBT-default-job1",
            "/tr",
            '"C:\\Python\\python.exe" -m wcbt scheduled-backup --profile default --job-id job1',
            "/sc",
            "DAILY",
            "/st",
            "06:30",
            "/it",
            "/rl",
            "LIMITED",
        ]
    ]


def test_create_task_builds_weekly_day_list() -> None:
    seen: list[list[str]] = []

    def _runner(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del kwargs
        command = args[0]
        assert isinstance(command, Iterable)
        seen.append([str(part) for part in command])
        return _completed(stdout="SUCCESS")

    backend = SchtasksBackend(runner=_runner)
    backend.create_task(
        task_name="WCBT-default-job1",
        task_command="python -m wcbt scheduled-backup",
        cadence="weekly",
        start_time_local="21:15",
        weekdays=("MON", "FRI"),
    )

    assert seen[0][-2:] == ["/d", "MON,FRI"]


def test_query_task_parses_verbose_output() -> None:
    backend = SchtasksBackend(
        runner=lambda *args, **kwargs: _completed(
            stdout="TaskName: \\WCBT\r\nNext Run Time: 4/13/2026 6:30:00 AM\r\n"
        )
    )

    result = backend.query_task(task_name="WCBT-default-job1")

    assert result.exists is True
    assert result.details["TaskName"] == "\\WCBT"
    assert result.details["Next Run Time"] == "4/13/2026 6:30:00 AM"


def test_query_task_returns_exists_false_for_missing_task() -> None:
    backend = SchtasksBackend(
        runner=lambda *args, **kwargs: _completed(
            returncode=1,
            stderr="ERROR: The system cannot find the file specified.",
        )
    )

    result = backend.query_task(task_name="WCBT-default-job1")

    assert result.exists is False
    assert result.details == {}


def test_run_task_raises_not_found_for_missing_task() -> None:
    backend = SchtasksBackend(
        runner=lambda *args, **kwargs: _completed(
            returncode=1,
            stderr="ERROR: The system cannot find the file specified.",
        )
    )

    with pytest.raises(ScheduledTaskNotFoundError):
        backend.run_task(task_name="WCBT-default-job1")


def test_backend_raises_generic_error_for_other_failures() -> None:
    backend = SchtasksBackend(
        runner=lambda *args, **kwargs: _completed(returncode=1, stderr="ERROR: Access is denied.")
    )

    with pytest.raises(SchedulingBackendError):
        backend.run_task(task_name="WCBT-default-job1")
