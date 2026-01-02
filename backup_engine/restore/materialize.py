from __future__ import annotations

import re
from typing import Any, Mapping

from .data_models import RestoreCandidate, RestoreOperationType, RestorePlan
from .errors import RestoreMaterializationError

_SPLIT_RELATIVE_PATH = re.compile(r"[\\/]+")


def _relative_path_to_parts(relative_path: str) -> list[str]:
    """
    Split a manifest relative_path into safe path parts.

    Parameters
    ----------
    relative_path:
        Manifest-relative path string. Both Windows-style (``nested\\b.txt``) and POSIX
        (``nested/b.txt``) separators are accepted.

    Returns
    -------
    list[str]
        Path components suitable for safe ``Path.joinpath(*parts)`` usage.

    Raises
    ------
    RestoreMaterializationError
        If the path is empty, cannot be split into parts, contains traversal (``.`` or ``..``),
        or contains disallowed segments (for example, colon characters).
    """
    raw = relative_path.strip()
    if raw == "":
        raise RestoreMaterializationError("relative_path must be non-empty.")

    parts = [p for p in _SPLIT_RELATIVE_PATH.split(raw) if p]
    if not parts:
        raise RestoreMaterializationError(f"relative_path is invalid: {relative_path!r}")

    # Enforce invariants: no absolute paths, no traversal.
    for part in parts:
        if part in (".", ".."):
            raise RestoreMaterializationError(
                f"relative_path contains traversal: {relative_path!r}"
            )
        if ":" in part:
            # Conservative: block drive-like segments and other colon uses.
            raise RestoreMaterializationError(
                f"relative_path contains invalid segment: {relative_path!r}"
            )

    return parts


def _operation_relative_path(operation: Mapping[str, Any]) -> str:
    """
    Extract and validate the operation relative_path field.

    Parameters
    ----------
    operation:
        Run manifest operation mapping.

    Returns
    -------
    str
        Operation relative path.

    Raises
    ------
    RestoreMaterializationError
        If the operation is missing a string ``relative_path`` field.
    """
    rel = operation.get("relative_path")
    if not isinstance(rel, str):
        raise RestoreMaterializationError("Operation missing string 'relative_path'.")
    return rel


def materialize_restore_candidates(plan: RestorePlan) -> list[RestoreCandidate]:
    """
    Materialize a deterministic list of restore candidates from a RestorePlan.

    Parameters
    ----------
    plan:
        RestorePlan previously built from a run manifest.

    Returns
    -------
    list[RestoreCandidate]
        Restore candidates in deterministic order (same order as source manifest operations).

    Raises
    ------
    RestoreMaterializationError
        If required internal plan fields are missing or invalid.

    Notes
    -----
    The RestorePlan carries a minimal manifest summary. The service layer provides the full
    operations list via the internal ``_operations_full`` field to keep planning deterministic
    while avoiding duplicating large manifest payloads.
    """
    operations_any = plan.source_manifest.get("operations_count")
    _ = operations_any  # documented count; actual ops come from the source manifest on disk via service layer

    # RestorePlan keeps only a minimal manifest summary for stability and size.
    # The service layer injects the full operations list via an internal field so that
    # candidate materialization remains deterministic without duplicating the manifest.
    run_ops = plan.source_manifest.get("_operations_full")
    if not isinstance(run_ops, list):
        raise RestoreMaterializationError(
            "RestorePlan missing _operations_full list (internal invariant)."
        )

    candidates: list[RestoreCandidate] = []
    for idx, op_any in enumerate(run_ops):
        if not isinstance(op_any, dict):
            raise RestoreMaterializationError(f"Operation at index {idx} must be an object.")
        rel = _operation_relative_path(op_any)

        parts = _relative_path_to_parts(rel)
        source_path = plan.archive_root.joinpath(*parts)
        destination_path = plan.destination_root.joinpath(*parts)

        if destination_path.exists():
            if plan.mode.value == "add-only":
                operation_type = RestoreOperationType.SKIP_EXISTING
                reason = "Destination exists; add-only mode plans a skip."
            else:
                operation_type = RestoreOperationType.OVERWRITE_EXISTING
                reason = "Destination exists; overwrite mode plans an overwrite."
        else:
            operation_type = RestoreOperationType.COPY_NEW
            reason = "Destination does not exist; normal copy."

        candidates.append(
            RestoreCandidate(
                operation_index=idx,
                relative_path=rel,
                source_path=source_path,
                destination_path=destination_path,
                operation_type=operation_type,
                reason=reason,
            )
        )

    return candidates
