"""
Source tree scanning for backup planning.

This module enumerates files under a source root in a deterministic manner and
collects metadata needed for planning. It performs no writes and never deletes.

Policy
------
- Symlinks/reparse points are skipped by default.
- Enumeration order is deterministic: directory names and file names are sorted.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SourceFileEntry:
    """
    Metadata for a source file discovered during scanning.

    Attributes
    ----------
    relative_path:
        Path of the file relative to the scan root.
    absolute_path:
        Fully resolved absolute path to the file.
    size_bytes:
        File size in bytes.
    modified_time_epoch_seconds:
        File modification time as seconds since epoch.
    """

    relative_path: Path
    absolute_path: Path
    size_bytes: int
    modified_time_epoch_seconds: float


@dataclass(frozen=True, slots=True)
class ScanRules:
    """
    Rules that control how a source tree is scanned.

    Attributes
    ----------
    excluded_directory_names:
        Directory names excluded from traversal anywhere in the tree.
        Matches are performed on the directory name only (not full paths).
    excluded_file_names:
        File names excluded anywhere in the tree.
        Matches are performed on the file name only (not full paths).
    """

    excluded_directory_names: frozenset[str]
    excluded_file_names: frozenset[str]


DEFAULT_EXCLUDED_DIRECTORY_NAMES: frozenset[str] = frozenset(
    {
        ".venv",
        ".git",
        "__pycache__",
        ".ruff_cache",
        ".mypy_cache",
        ".pytest_cache",
        ".idea",
        ".vscode",
        ".vs",
    }
)

DEFAULT_EXCLUDED_FILE_NAMES: frozenset[str] = frozenset()

DEFAULT_SCAN_RULES = ScanRules(
    excluded_directory_names=DEFAULT_EXCLUDED_DIRECTORY_NAMES,
    excluded_file_names=DEFAULT_EXCLUDED_FILE_NAMES,
)


@dataclass(frozen=True, slots=True)
class ScanIssue:
    """
    A non-fatal issue encountered while scanning.

    Attributes
    ----------
    path:
        The path that triggered the issue.
    message:
        Human-readable explanation suitable for logs or dry-run output.
    """

    path: Path
    message: str


@dataclass(frozen=True, slots=True)
class ScanResult:
    """
    Result of scanning a source tree.

    Attributes
    ----------
    entries:
        Discovered file entries.
    issues:
        Non-fatal issues encountered during scanning.
    """

    entries: list[SourceFileEntry]
    issues: list[ScanIssue]


def scan_source_tree(source_root: Path, rules: ScanRules) -> ScanResult:
    """
    Scan a source directory recursively and return file entries.

    Parameters
    ----------
    source_root:
        Root directory to scan. Must be an existing directory.
    rules:
        Rules controlling directory and file exclusion.

    Returns
    -------
    ScanResult
        Deterministic set of discovered files and any non-fatal scan issues.

    Notes
    -----
    - This function does not follow symlinks.
    - This function performs no filesystem writes.
    """
    resolved_source_root = source_root.resolve(strict=True)
    entries: list[SourceFileEntry] = []
    issues: list[ScanIssue] = []

    for directory_path, directory_names, file_names in os.walk(
        resolved_source_root,
        topdown=True,
        followlinks=False,
    ):
        directory_names[:] = [
            name for name in directory_names if name not in rules.excluded_directory_names
        ]
        directory_names.sort()
        file_names.sort()

        current_directory = Path(directory_path)

        for file_name in file_names:
            absolute_path = current_directory / file_name

            if absolute_path.name in rules.excluded_file_names:
                continue

            try:
                # Skip symlinks/reparse points; safer default.
                if absolute_path.is_symlink():
                    issues.append(
                        ScanIssue(
                            path=absolute_path,
                            message="Skipped symlink/reparse point.",
                        )
                    )
                    continue

                stat_result = absolute_path.stat()
                relative_path = absolute_path.relative_to(resolved_source_root)

                # Defensive invariant checks: relative paths must be safe and truly relative.
                if relative_path.is_absolute() or ".." in relative_path.parts:
                    issues.append(
                        ScanIssue(
                            path=absolute_path,
                            message=f"Unsafe relative path derived: {relative_path}",
                        )
                    )
                    continue

                entries.append(
                    SourceFileEntry(
                        relative_path=relative_path,
                        absolute_path=absolute_path,
                        size_bytes=int(stat_result.st_size),
                        modified_time_epoch_seconds=float(stat_result.st_mtime),
                    )
                )
            except OSError as exc:
                issues.append(
                    ScanIssue(
                        path=absolute_path,
                        message=f"Failed to stat file: {exc!s}",
                    )
                )
                continue

    return ScanResult(entries=entries, issues=issues)
