"""Profile initialization operations.

This module implements the non-destructive "init" workflow used during the dry-run
phase. It creates the required directory structure for a profile inside the WCBT
data root and can optionally print the resolved paths.

No file deletion is performed by this module.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from .paths_and_safety import ProfilePaths, ensure_profile_directories, resolve_profile_paths


def init_profile(profile_name: str, data_root: Path | None = None) -> ProfilePaths:
    """Initialize (create) the directory structure for a profile.

    Parameters
    ----------
    profile_name:
        Name of the profile to initialize.
    data_root:
        Optional override for the WCBT data root. If not provided, the default
        data root resolver is used.

    Returns
    -------
    ProfilePaths
        The resolved profile paths that were initialized.
    """
    paths = resolve_profile_paths(profile_name=profile_name, data_root=data_root)
    ensure_profile_directories(paths)
    return paths


def profile_paths_as_text(paths: ProfilePaths) -> str:
    """Render ProfilePaths as a readable multi-line string.

    Parameters
    ----------
    paths:
        Profile paths to render.

    Returns
    -------
    str
        Human-friendly string representation of the resolved paths.
    """
    items = asdict(paths)
    lines: list[str] = []
    for key in (
        "data_root",
        "profile_root",
        "manifests_root",
        "archives_root",
        "index_root",
        "logs_root",
        "work_root",
        "live_snapshots_root",
    ):
        lines.append(f"{key}: {items[key]}")
    return "\n".join(lines)
