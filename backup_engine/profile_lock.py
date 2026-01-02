"""
Profile-level locking for WCBT.

This module provides a conservative, Windows-first file lock used to prevent
concurrent materialize/execute operations for the same backup profile.

Design goals
------------
- Deterministic and inspectable: locks are plain JSON written atomically.
- Safe by default: existing locks block unless explicitly overridden.
- Conservative stale detection: a lock is provably stale only when we can prove
  the recorded PID is not running on the same host.

Notes
-----
This uses exclusive file creation and explicit lock breaking rules. That keeps
behavior predictable across Python versions and filesystem types without adding
third-party dependencies.
"""

from __future__ import annotations

import json
import os
import platform
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from backup_engine.errors import WcbtError


class ProfileLockError(WcbtError):
    """
    Raised when acquiring, releasing, or breaking a profile lock fails.

    The error message is intended to be user-facing and should explain how to
    proceed (e.g., using --force or --break-lock) without requiring stack traces.
    """


@dataclass(frozen=True, slots=True)
class ProfileLockInfo:
    """
    Metadata recorded in a profile lock file.

    Attributes
    ----------
    schema_version:
        Schema identifier for the lock JSON.
    profile_name:
        Profile name the lock applies to.
    created_at_utc:
        Lock acquisition time in UTC (ISO 8601 with 'Z').
    hostname:
        Hostname where the lock was created.
    pid:
        Process ID of the creating process.
    command:
        High-level command name, e.g., "backup".
    run_id:
        Optional run/archive identifier associated with the lock.
    """

    schema_version: str
    profile_name: str
    created_at_utc: str
    hostname: str
    pid: int
    command: str
    run_id: str | None = None


def build_profile_lock_path(*, work_root: Path) -> Path:
    """
    Build the filesystem path for the profile lock file.

    Parameters
    ----------
    work_root:
        Profile work root directory.

    Returns
    -------
    Path
        Path to the lock file.
    """
    return work_root / "locks" / "backup.lock"


@contextmanager
def acquire_profile_lock(
    *,
    lock_path: Path,
    profile_name: str,
    command: str,
    run_id: str | None,
    force: bool,
    break_lock: bool,
) -> Iterator[None]:
    """
    Acquire a profile lock for commands that must not run concurrently per profile.

    Parameters
    ----------
    lock_path:
        Filesystem path of the lock file to acquire.
    profile_name:
        Profile name for reporting and validation.
    command:
        High-level command name, e.g., "backup".
    run_id:
        Optional run/archive identifier for inspection.
    force:
        If True, break the lock only when it is provably stale (same host, dead PID).
    break_lock:
        If True, break an existing lock even when it is not provably stale.

    Raises
    ------
    ProfileLockError
        If the lock is held and cannot be broken under the provided flags, or if
        the lock cannot be acquired or released.
    """
    lock_path = lock_path.expanduser()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    info = _build_lock_info(profile_name=profile_name, command=command, run_id=run_id)

    # Fast path: exclusive create.
    try:
        _write_lock_exclusive(lock_path, info)
        try:
            yield
        finally:
            _release_lock(lock_path, info)
        return
    except FileExistsError:
        pass

    existing = _try_read_lock(lock_path)
    decision = _evaluate_existing_lock(existing=existing, force=force, break_lock=break_lock)
    if not decision.allow_break:
        raise ProfileLockError(decision.message)

    # Break then acquire.
    try:
        lock_path.unlink(missing_ok=True)
    except OSError as exc:
        raise ProfileLockError(f"Failed to remove existing lock: {lock_path} ({exc})") from exc

    try:
        _write_lock_exclusive(lock_path, info)
    except FileExistsError:
        # A race: someone else acquired between unlink and create.
        raise ProfileLockError(f"Lock is held by another process: {lock_path}")

    try:
        yield
    finally:
        _release_lock(lock_path, info)


def _build_lock_info(*, profile_name: str, command: str, run_id: str | None) -> ProfileLockInfo:
    created = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return ProfileLockInfo(
        schema_version="wcbt_profile_lock_v1",
        profile_name=profile_name,
        created_at_utc=created,
        hostname=platform.node(),
        pid=os.getpid(),
        command=command,
        run_id=run_id,
    )


def _write_lock_exclusive(lock_path: Path, info: ProfileLockInfo) -> None:
    payload = json.dumps(asdict(info), sort_keys=True) + "\n"
    with lock_path.open("x", encoding="utf-8", newline="\n") as f:
        f.write(payload)


def _release_lock(lock_path: Path, info: ProfileLockInfo) -> None:
    """
    Release the lock if it still appears to be held by this process.

    Notes
    -----
    This is a best-effort safety check: we read the file and confirm PID/hostname
    before unlinking. If the lock content is unreadable, we still attempt to remove
    it to avoid leaving a dead lock behind after successful command completion.
    """
    try:
        existing = _try_read_lock(lock_path)
        if existing is not None:
            same_owner = (
                str(existing.get("pid")) == str(info.pid)
                and str(existing.get("hostname", "")).lower() == str(info.hostname).lower()
            )
            if not same_owner:
                return
        lock_path.unlink(missing_ok=True)
    except OSError as exc:
        raise ProfileLockError(f"Failed to release lock: {lock_path} ({exc})") from exc


def _try_read_lock(lock_path: Path) -> Mapping[str, object] | None:
    try:
        raw = lock_path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    return data


@dataclass(frozen=True, slots=True)
class _BreakDecision:
    allow_break: bool
    message: str


def _evaluate_existing_lock(
    *,
    existing: Mapping[str, object] | None,
    force: bool,
    break_lock: bool,
) -> _BreakDecision:
    details = _format_lock_details(existing)
    if existing is None:
        if break_lock:
            return _BreakDecision(True, "Breaking lock with unreadable metadata.")
        return _BreakDecision(
            False,
            "Lock exists but could not be read. Inspect the lock file and re-run with --break-lock if necessary.\n"
            + details,
        )

    provably_stale = _is_provably_stale(existing)
    if provably_stale:
        if force:
            return _BreakDecision(True, "Breaking provably stale lock due to --force.")
        return _BreakDecision(
            False, "Lock appears to be stale. Re-run with --force to break it.\n" + details
        )

    if break_lock:
        return _BreakDecision(True, "Breaking lock due to --break-lock.")
    return _BreakDecision(
        False,
        "Lock is held and is not provably stale. Re-run with --break-lock to override.\n" + details,
    )


def _format_lock_details(existing: Mapping[str, object] | None) -> str:
    if not existing:
        return ""
    fields = ["profile_name", "created_at_utc", "hostname", "pid", "command", "run_id"]
    parts: list[str] = []
    for field in fields:
        if field in existing:
            parts.append(f"{field}={existing.get(field)!r}")
    return "Lock details: " + ", ".join(parts) if parts else ""


def _is_provably_stale(existing: Mapping[str, object]) -> bool:
    host = existing.get("hostname")
    pid = existing.get("pid")

    if not isinstance(host, str):
        return False
    if not isinstance(pid, int):
        return False

    if host.lower() != platform.node().lower():
        return False

    running = is_pid_running(pid)
    if running is None:
        return False
    return running is False


def is_pid_running(pid: int) -> bool | None:
    """
    Determine whether a process is running.

    Parameters
    ----------
    pid:
        Process ID to check.

    Returns
    -------
    bool | None
        True if running, False if not running, None if indeterminate or unsupported.

    Raises
    ------
    None
    """
    if os.name != "nt":
        return None
    return _is_pid_running_windows(pid)


def _is_pid_running_windows(pid: int) -> bool | None:
    """
    Check whether a process is running on Windows using the Win32 API.

    Parameters
    ----------
    pid:
        Process ID to check.

    Returns
    -------
    bool | None
        True if running, False if not running, None if indeterminate (e.g., access denied).

    Notes
    -----
    This function is intentionally conservative. If the process state cannot be
    determined (for example, access denied), it returns None rather than guessing.
    """
    import ctypes
    from ctypes import wintypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    open_process = kernel32.OpenProcess
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE

    get_exit_code_process = kernel32.GetExitCodeProcess
    get_exit_code_process.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    get_exit_code_process.restype = wintypes.BOOL

    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    handle = open_process(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        # Could be not running or access denied. Be conservative.
        return None

    try:
        exit_code = wintypes.DWORD()
        ok = get_exit_code_process(handle, ctypes.byref(exit_code))
        if not ok:
            return None
        return exit_code.value == STILL_ACTIVE
    finally:
        close_handle(handle)
