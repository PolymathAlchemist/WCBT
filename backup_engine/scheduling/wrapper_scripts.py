"""Per-job wrapper-script generation for Windows scheduled execution."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

import wcbt
from backup_engine.errors import SchedulingBackendError
from backup_engine.paths_and_safety import resolve_profile_paths


@dataclass(frozen=True, slots=True)
class ScheduledWrapperPaths:
    """
    Filesystem paths used by one generated scheduled-run wrapper.

    Attributes
    ----------
    wrapper_path:
        Batch file executed by Windows Task Scheduler.
    log_path:
        Append-only log file written by the wrapper.
    runtime_root:
        Resolved WCBT runtime root used as the wrapper working directory.
    """

    wrapper_path: Path
    log_path: Path
    runtime_root: Path


def resolve_runtime_root() -> Path:
    """
    Return the runtime root that contains the active WCBT package.

    Returns
    -------
    Path
        Absolute path to the current WCBT runtime root.

    Notes
    -----
    ``wcbt.__file__`` points into the active package location, whether WCBT is
    being run from a repo checkout or an installed environment. The runtime
    root is the package parent so scheduled wrappers follow the real local
    machine location instead of assuming a fixed path.
    """

    return Path(wcbt.__file__).resolve().parent.parent


def scheduled_wrapper_paths(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
) -> ScheduledWrapperPaths:
    """
    Resolve the wrapper and log paths for one scheduled job.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the scheduled job.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.

    Returns
    -------
    ScheduledWrapperPaths
        Resolved wrapper, log, and runtime root paths.
    """

    profile_paths = resolve_profile_paths(profile_name=profile_name, data_root=data_root)
    wrapper_root = profile_paths.profile_root / "scheduled_wrappers"
    log_root = profile_paths.logs_root / "scheduled"
    return ScheduledWrapperPaths(
        wrapper_path=wrapper_root / f"{job_id}.bat",
        log_path=log_root / f"{job_id}.log",
        runtime_root=resolve_runtime_root(),
    )


def write_scheduled_job_wrapper(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
) -> Path:
    """
    Write or update the per-job scheduled-run wrapper batch file.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the scheduled job.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.

    Returns
    -------
    Path
        Path to the generated wrapper batch file.

    Raises
    ------
    SchedulingBackendError
        If the wrapper cannot be written or verified on disk.
    """

    paths = scheduled_wrapper_paths(profile_name=profile_name, data_root=data_root, job_id=job_id)
    paths.wrapper_path.parent.mkdir(parents=True, exist_ok=True)
    paths.log_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "uv",
        "run",
        "python",
        "-m",
        "wcbt",
        "run-job",
        "--profile",
        profile_name,
        "--job-id",
        job_id,
        "--mode",
        "execute-compress",
    ]
    if data_root is not None:
        command.extend(["--data-root", str(data_root)])

    lines = [
        "@echo off",
        "setlocal",
        f'cd /d "{paths.runtime_root}" || exit /b 1',
        f'if not exist "{paths.log_path.parent}" mkdir "{paths.log_path.parent}" >nul 2>&1',
        f'{subprocess.list2cmdline(command)} >> "{paths.log_path}" 2>&1',
        "exit /b %ERRORLEVEL%",
        "",
    ]
    try:
        paths.wrapper_path.write_text("\r\n".join(lines), encoding="utf-8")
    except OSError as exc:
        raise SchedulingBackendError(
            f"Failed to write scheduled wrapper at {paths.wrapper_path}: {exc}"
        ) from exc
    if not paths.wrapper_path.is_file():
        raise SchedulingBackendError(f"Scheduled wrapper was not created at: {paths.wrapper_path}")
    return paths.wrapper_path


def build_schtasks_wrapper_command(wrapper_path: Path) -> str:
    """
    Build the Windows Task Scheduler action command for a wrapper batch file.

    Parameters
    ----------
    wrapper_path:
        Absolute wrapper batch path to execute.

    Returns
    -------
    str
        Command string suitable for ``schtasks /tr``.

    Notes
    -----
    Using ``cmd.exe /c`` keeps Task Scheduler behavior stable for ``.bat``
    wrappers instead of depending on direct batch-file execution semantics.
    """
    return subprocess.list2cmdline(["cmd.exe", "/c", str(wrapper_path)])


def delete_scheduled_job_wrapper(
    *,
    profile_name: str,
    data_root: Path | None,
    job_id: str,
) -> None:
    """
    Delete a generated wrapper script for a scheduled job if it exists.

    Parameters
    ----------
    profile_name:
        WCBT profile that owns the scheduled job.
    data_root:
        Optional WCBT data root override.
    job_id:
        Stable WCBT job identifier.
    """

    wrapper_path = scheduled_wrapper_paths(
        profile_name=profile_name,
        data_root=data_root,
        job_id=job_id,
    ).wrapper_path
    if wrapper_path.exists():
        wrapper_path.unlink()
