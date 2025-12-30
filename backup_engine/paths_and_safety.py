"""
Filesystem path policy and safety gates.

This module is the single choke point for determining where WCBT is allowed
to read and write data. It enforces a safety-first posture:

- Runtime data lives under a WCBT "data root" (default: %APPDATA%\\wcbt).
- WCBT never deletes outside of its own work directory (v1: no deletion at all).
- Restore targets must be explicitly validated and must not point to unsafe paths.

Nothing in the engine should perform filesystem writes or destructive operations
without going through this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProfilePaths:
    """
    Concrete resolved paths for a WCBT profile.

    Attributes
    ----------
    data_root:
        The root directory for all WCBT runtime data.
    profile_root:
        Root for the named profile within `data_root`.
    work_root:
        Temporary working directory for staging, extract temp, etc.
        This is the only directory that may ever be eligible for cleanup.
        For v1, cleanup deletion is disabled by policy.
    manifests_root:
        Canonical JSON manifests (source of truth).
    archives_root:
        Backup archives (tar.zst/zip/etc).
    index_root:
        SQLite index and related rebuildable files.
    logs_root:
        Structured logs.
    live_snapshots_root:
        Safety snapshots taken before restore operations.
    """

    data_root: Path
    profile_root: Path
    work_root: Path
    manifests_root: Path
    archives_root: Path
    index_root: Path
    logs_root: Path
    live_snapshots_root: Path


class SafetyViolationError(RuntimeError):
    """Raised when an operation is blocked by safety policy."""


def default_data_root() -> Path:
    """
    Resolve the default WCBT data root on Windows.

    Preference order:
    1) %LOCALAPPDATA% if set
    2) %APPDATA% (Roaming) as fallback
    """
    local = os.environ.get("LOCALAPPDATA")
    if local:
        return Path(local) / "wcbt"

    roaming = os.environ.get("APPDATA")
    if roaming:
        return Path(roaming) / "wcbt"

    raise SafetyViolationError("Neither LOCALAPPDATA nor APPDATA environment variables are set.")


def resolve_profile_paths(profile_name: str, data_root: Path | None = None) -> ProfilePaths:
    """
    Resolve and return all filesystem paths for a given profile.

    Parameters
    ----------
    profile_name:
        Name of the profile. Must be a non-empty, simple folder name.
    data_root:
        Optional override for the WCBT data root.

    Returns
    -------
    ProfilePaths
        Resolved profile paths.

    Raises
    ------
    SafetyViolationError
        If profile_name is unsafe or resolution violates safety rules.
    """
    profile = profile_name.strip()
    if not profile:
        raise SafetyViolationError("Profile name must not be empty.")
    if any(ch in profile for ch in r'\/:*?"<>|'):
        raise SafetyViolationError(f"Profile name contains invalid characters: {profile!r}")
    if profile in {".", ".."}:
        raise SafetyViolationError("Profile name must not be '.' or '..'.")

    root = (data_root or default_data_root()).resolve()

    profile_root = (root / "profiles" / profile).resolve()
    work_root = (profile_root / "work").resolve()
    manifests_root = (profile_root / "manifests").resolve()
    archives_root = (profile_root / "archives").resolve()
    index_root = (profile_root / "index").resolve()
    logs_root = (profile_root / "logs").resolve()
    live_snapshots_root = (profile_root / "live_snapshots").resolve()

    _assert_within(root, profile_root, purpose="profile root")
    return ProfilePaths(
        data_root=root,
        profile_root=profile_root,
        work_root=work_root,
        manifests_root=manifests_root,
        archives_root=archives_root,
        index_root=index_root,
        logs_root=logs_root,
        live_snapshots_root=live_snapshots_root,
    )


def ensure_profile_directories(paths: ProfilePaths) -> None:
    """
    Create the directory structure for a profile if it does not already exist.

    Parameters
    ----------
    paths:
        Resolved profile paths.

    Notes
    -----
    This function creates directories only. It performs no deletion.
    """
    for directory in (
        paths.profile_root,
        paths.work_root,
        paths.manifests_root,
        paths.archives_root,
        paths.index_root,
        paths.logs_root,
        paths.live_snapshots_root,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def validate_restore_target(server_root: Path, world_folder: str) -> Path:
    """
    Validate and return the canonical restore target directory for a world.

    Parameters
    ----------
    server_root:
        Root directory of the dedicated server instance.
    world_folder:
        Canonical world folder name, for example "world".

    Returns
    -------
    pathlib.Path
        The full path to the world folder under server_root.

    Raises
    ------
    SafetyViolationError
        If the target is unsafe or world_folder is invalid.
    """
    folder = world_folder.strip()
    if not folder or folder in {".", ".."}:
        raise SafetyViolationError("world_folder must be a simple non-empty folder name.")
    if any(sep in folder for sep in ("/", "\\", ":")):
        raise SafetyViolationError("world_folder must not contain path separators or drive hints.")

    server_root_resolved = server_root.resolve()
    target = (server_root_resolved / folder).resolve()

    _assert_not_system_path(target)
    _assert_min_depth(target, min_parts=3, purpose="restore target")
    _assert_within(server_root_resolved, target, purpose="restore target within server root")

    return target


def _assert_within(base: Path, candidate: Path, purpose: str) -> None:
    """Ensure candidate is within base after resolution."""
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise SafetyViolationError(
            f"Unsafe path for {purpose}: {candidate} is not within {base}"
        ) from exc


def _assert_min_depth(path: Path, min_parts: int, purpose: str) -> None:
    """Block obviously dangerous shallow targets like C:\\ or D:\\."""
    parts = path.resolve().parts
    if len(parts) < min_parts:
        raise SafetyViolationError(
            f"Unsafe path for {purpose}: {path} is too shallow ({len(parts)} parts)."
        )


def _assert_not_system_path(path: Path) -> None:
    """
    Block operations that target obvious system directories.

    Notes
    -----
    This is conservative by design. It is intended to prevent "oops" class failures.
    """
    p = path.resolve()
    p_str = str(p).lower()

    blocked_prefixes = (
        r"c:\windows",
        r"c:\program files",
        r"c:\program files (x86)",
    )

    if p_str == r"c:\\" or p_str == r"c:":
        raise SafetyViolationError("Refusing to operate on drive root C:\\")

    for prefix in blocked_prefixes:
        if p_str.startswith(prefix):
            raise SafetyViolationError(f"Refusing to operate on system path: {p}")


def validate_source_path(source: Path) -> Path:
    """Validate a backup source path and return its resolved absolute form.

    Safety goals:
    - Must exist and be a directory
    - Disallow filesystem roots (C:\\, D:\\, etc.) by default
    - Normalize to an absolute resolved path

    Parameters
    ----------
    source:
        Candidate source directory.

    Returns
    -------
    pathlib.Path
        Resolved absolute source directory.

    Raises
    ------
    SafetyViolationError
        If the source path does not exist, is not a directory, or is unsafe.
    """
    source = Path(source).expanduser()

    try:
        resolved = source.resolve(strict=True)
    except FileNotFoundError as exc:
        raise SafetyViolationError(f"Source path does not exist: {source}") from exc

    if not resolved.is_dir():
        raise SafetyViolationError(f"Source path is not a directory: {resolved}")

    if len(resolved.parts) <= 1:
        raise SafetyViolationError(f"Refusing to use filesystem root as source: {resolved}")

    return resolved
