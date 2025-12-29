"""
Backup planning for WCBT.

This module converts a scan result into a deterministic plan of operations.
In v1, planning is dry-run only and includes no deletion.

Design Notes
------------
For archive-based backups where each run uses a new archive ID, the destination
is expected to be empty. Therefore all discovered source files plan as COPY.
Change detection will be introduced later via manifest/index comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from backup_engine.backup.scan import ScanIssue, SourceFileEntry
from backup_engine.errors import BackupError


class PlannedOperationType(Enum):
    """
    Planned operation identifiers.

    Notes
    -----
    This enum is intentionally explicit and stable; it is part of the public
    domain model for planning and reporting.
    """

    COPY_FILE_TO_ARCHIVE = "copy_file_to_archive"
    SKIP_UNSAFE_PATH = "skip_unsafe_path"


@dataclass(frozen=True, slots=True)
class PlannedOperation:
    """
    A single planned operation for a backup.

    Attributes
    ----------
    operation_type:
        Type of operation.
    source_path:
        Absolute path to the source file.
    destination_path:
        Absolute path to the planned destination within the archive root.
    relative_path:
        Source path relative to the source root; used for stable sorting and reporting.
    reason:
        Explanation for why this operation exists (human-facing).
    """

    operation_type: PlannedOperationType
    source_path: Path
    destination_path: Path
    relative_path: Path
    reason: str


@dataclass(frozen=True, slots=True)
class BackupPlan:
    """
    A deterministic backup plan.

    Attributes
    ----------
    archive_root:
        The planned archive destination root for this run.
    operations:
        Planned operations in deterministic order.
    scan_issues:
        Non-fatal scan issues carried into planning output.
    """

    archive_root: Path
    operations: list[PlannedOperation]
    scan_issues: list[ScanIssue]


def build_backup_plan(
    *,
    entries: list[SourceFileEntry],
    archive_root: Path,
) -> BackupPlan:
    """
    Build a deterministic backup plan for an archive-based backup.

    Parameters
    ----------
    entries:
        Discovered source files from scanning.
    archive_root:
        Planned archive destination root. No directories are created here.

    Returns
    -------
    BackupPlan
        Deterministic plan of operations.

    Raises
    ------
    BackupError
        If planning encounters an unrecoverable invariant violation.

    Invariants
    ----------
    - Destination paths are always within archive_root.
    - Relative paths must not be absolute and must not contain parent traversal.
    """
    resolved_archive_root = archive_root.resolve()

    operations: list[PlannedOperation] = []
    for entry in _sorted_entries(entries):
        relative_path = entry.relative_path

        if _is_unsafe_relative_path(relative_path):
            operations.append(
                PlannedOperation(
                    operation_type=PlannedOperationType.SKIP_UNSAFE_PATH,
                    source_path=entry.absolute_path,
                    destination_path=resolved_archive_root,
                    relative_path=relative_path,
                    reason="Unsafe relative path (absolute path or parent traversal).",
                )
            )
            continue

        destination_path = (resolved_archive_root / relative_path).resolve()

        if not _is_within_base(resolved_archive_root, destination_path):
            raise BackupError(
                f"Planned destination escaped archive root. archive_root={resolved_archive_root} "
                f"destination={destination_path} relative={relative_path}"
            )

        operations.append(
            PlannedOperation(
                operation_type=PlannedOperationType.COPY_FILE_TO_ARCHIVE,
                source_path=entry.absolute_path,
                destination_path=destination_path,
                relative_path=relative_path,
                reason="Archive-based backup plans all discovered files as copies (v1).",
            )
        )

    return BackupPlan(archive_root=resolved_archive_root, operations=operations, scan_issues=[])


def _sorted_entries(entries: list[SourceFileEntry]) -> list[SourceFileEntry]:
    """
    Return entries sorted deterministically by relative path.

    Parameters
    ----------
    entries:
        Entries to sort.

    Returns
    -------
    list[SourceFileEntry]
        Sorted entries.
    """
    return sorted(entries, key=lambda entry: str(entry.relative_path).lower())


def _is_unsafe_relative_path(relative_path: Path) -> bool:
    """
    Determine whether a relative path violates safety constraints.

    Parameters
    ----------
    relative_path:
        Candidate relative path.

    Returns
    -------
    bool
        True if unsafe, False otherwise.
    """
    if relative_path.is_absolute():
        return True
    return any(part in (".", "..") for part in relative_path.parts)


def _is_within_base(base: Path, candidate: Path) -> bool:
    """
    Check that candidate is within base.

    Parameters
    ----------
    base:
        Base directory.
    candidate:
        Candidate path.

    Returns
    -------
    bool
        True if candidate is within base, False otherwise.
    """
    try:
        candidate.relative_to(base)
        return True
    except ValueError:
        return False


def attach_scan_issues(plan: BackupPlan, issues: list[ScanIssue]) -> BackupPlan:
    """
    Return a new plan with scan issues attached.

    Parameters
    ----------
    plan:
        Existing plan.
    issues:
        Scan issues to attach.

    Returns
    -------
    BackupPlan
        New plan with scan issues included.
    """
    return BackupPlan(archive_root=plan.archive_root, operations=plan.operations, scan_issues=issues)


def serialize_plan_for_manifest(plan: BackupPlan) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """
    Serialize a plan into JSON-safe structures for inclusion in a run manifest.

    Parameters
    ----------
    plan:
        Backup plan to serialize.

    Returns
    -------
    tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]
        A pair of (operations_payload, scan_issues_payload), each suitable for JSON.

    Notes
    -----
    - Output order is deterministic and matches plan.operations order.
    - Paths are serialized as strings.
    """
    operations_payload: list[Mapping[str, Any]] = []
    for operation in plan.operations:
        operations_payload.append(
            {
                "operation_type": operation.operation_type.value,
                "source_path": str(operation.source_path),
                "destination_path": str(operation.destination_path),
                "relative_path": str(operation.relative_path),
                "reason": operation.reason,
            }
        )

    scan_issues_payload: list[Mapping[str, Any]] = []
    for issue in plan.scan_issues:
        scan_issues_payload.append(_serialize_scan_issue(issue))

    return operations_payload, scan_issues_payload


def _serialize_scan_issue(issue: ScanIssue) -> Mapping[str, Any]:
    """
    Serialize a ScanIssue into a JSON-safe structure.

    Parameters
    ----------
    issue:
        Scan issue to serialize.

    Returns
    -------
    Mapping[str, Any]
        JSON-ready mapping.

    Notes
    -----
    This intentionally uses a conservative representation to avoid coupling
    to ScanIssue internals. If ScanIssue evolves, this output can evolve with it.
    """
    payload: dict[str, Any] = {}

    issue_path = getattr(issue, "path", None)
    if issue_path is not None:
        payload["path"] = str(issue_path)

    payload["message"] = str(getattr(issue, "message", ""))
    payload["issue_type"] = str(getattr(issue, "issue_type", ""))

    return payload
