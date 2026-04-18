from __future__ import annotations

import errno
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from backup_engine.restore.errors import RestoreError
from backup_engine.restore.journal import RestoreExecutionJournal


@dataclass(frozen=True)
class PromotionPlan:
    """
    Planned atomic promotion operations.

    Attributes
    ----------
    stage_root : Path
        Directory containing staged restore data.
    target_root : Path
        Final restore destination.
    previous_root : Optional[Path]
        Location where an existing target would be moved, if present.
    operations : list[str]
        Human-readable ordered list of planned operations.
    """

    stage_root: Path
    target_root: Path
    previous_root: Optional[Path]
    operations: List[str]


@dataclass(frozen=True)
class PromotionOutcome:
    """
    Result of a promotion attempt.

    Attributes
    ----------
    promoted : bool
        True if stage was successfully promoted to target.
    previous_root : Optional[Path]
        Path to preserved previous target, if any.
    """

    promoted: bool
    previous_root: Optional[Path]


class PromotionError(RestoreError):
    """
    Raised when atomic promotion fails.

    Notes
    -----
    Promotion failures indicate that the staged tree could not be safely promoted
    to the destination, either due to filesystem state or mid-operation errors.
    """


def _is_winerror_32_permission_error(error: BaseException) -> bool:
    """
    Return whether an exception is the narrow Windows live-lock case.

    Parameters
    ----------
    error : BaseException
        Exception raised by a filesystem mutation.

    Returns
    -------
    bool
        True when the error is a ``PermissionError`` with ``winerror == 32``.
    """
    return isinstance(error, PermissionError) and getattr(error, "winerror", None) == 32


def _move_directory_with_cross_volume_fallback(
    *,
    source_root: Path,
    destination_root: Path,
    operation_label: str,
) -> None:
    """
    Move one promotion directory into place.

    Parameters
    ----------
    source_root:
        Existing directory that should be moved.
    destination_root:
        Target directory path for the move.
    operation_label:
        Short label describing the current promotion step.

    Raises
    ------
    PromotionError
        If the move fails. On Windows live-destination lock failures, this raises
        a lock-specific message that identifies the blocked rename operands.

    Notes
    -----
    Promotion currently relies on directory renames. This helper keeps that local
    behavior in one place so we can surface a clear lock-specific error for the
    live destination root without changing broader restore architecture.
    """
    try:
        source_root.rename(destination_root)
    except Exception as exc:  # noqa: BLE001
        if (
            operation_label == "preserve_existing_destination"
            and isinstance(exc, PermissionError)
            and getattr(exc, "winerror", None) == 32
        ):
            raise PromotionError(
                "Restore could not replace the live destination because another process "
                f"is using it. Close any app, terminal, Explorer window, or process "
                f"using '{source_root}' and retry. Rename blocked: '{source_root}' -> "
                f"'{destination_root}'."
            ) from exc

        if isinstance(exc, OSError) and exc.errno == errno.EXDEV:
            raise PromotionError(
                "Atomic promotion requires source and destination to be on the same "
                f"volume. Move blocked during {operation_label}: '{source_root}' -> "
                f"'{destination_root}'."
            ) from exc

        raise


def _promote_overwrite_filewise(*, stage_root: Path, target_root: Path) -> None:
    """
    Copy staged restore content into the live destination tree file by file.

    Parameters
    ----------
    stage_root : Path
        Directory containing verified staged restore content.
    target_root : Path
        Live restore destination directory to update in place.

    Raises
    ------
    PromotionError
        If a staged file cannot be copied into place. Windows lock failures
        report the exact blocked destination path.

    Notes
    -----
    This is a narrow fallback for Windows ``WinError 32`` on the initial
    destination-root preserve rename. It intentionally overlays the staged tree
    onto the live destination without changing the broader restore flow.
    """
    target_root.mkdir(parents=True, exist_ok=True)

    for staged_directory in sorted(
        (path for path in stage_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.relative_to(stage_root).parts),
    ):
        destination_directory = target_root / staged_directory.relative_to(stage_root)
        destination_directory.mkdir(parents=True, exist_ok=True)

    for staged_file in sorted(path for path in stage_root.rglob("*") if path.is_file()):
        destination_file = target_root / staged_file.relative_to(stage_root)
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(staged_file, destination_file)
        except Exception as exc:  # noqa: BLE001
            if _is_winerror_32_permission_error(exc):
                raise PromotionError(
                    "Restore could not overwrite the live destination file because "
                    f"another process is using the file: '{destination_file}'. "
                    "The pre-restore safeguard backup remains intact."
                ) from exc
            raise


def _derive_previous_root(target_root: Path, run_id: str) -> Path:
    """
    Derive a deterministic path for preserving a previous destination.

    Parameters
    ----------
    target_root:
        Final restore destination directory.
    run_id:
        Unique restore run identifier.

    Returns
    -------
    pathlib.Path
        Path where the previous destination would be preserved.
    """
    return target_root.with_name(f".wcbt_restore_previous_{target_root.name}_{run_id}")


def _validate_promotion_paths(
    *,
    stage_root: Path,
    target_root: Path,
    previous_root: Optional[Path],
) -> None:
    """
    Validate filesystem preconditions for atomic promotion.

    Parameters
    ----------
    stage_root:
        Directory containing staged restore content.
    target_root:
        Final restore destination directory.
    previous_root:
        Path where an existing destination would be moved, if applicable.

    Raises
    ------
    PromotionError
        If required paths do not exist, are of the wrong type, or would conflict.
    """
    if not stage_root.exists() or not stage_root.is_dir():
        raise PromotionError(f"Stage root does not exist or is not a directory: {stage_root}")

    if target_root.exists() and not target_root.is_dir():
        raise PromotionError(f"Target exists but is not a directory: {target_root}")

    if previous_root is not None and previous_root.exists():
        raise PromotionError(f"Previous root already exists: {previous_root}")


def plan_promotion(
    *,
    stage_root: Path,
    target_root: Path,
    run_id: str,
) -> PromotionPlan:
    """
    Plan an atomic promotion without mutating the filesystem.

    Parameters
    ----------
    stage_root : Path
        Directory containing staged restore data.
    target_root : Path
        Final restore destination.
    run_id : str
        Unique identifier for this restore run.

    Returns
    -------
    PromotionPlan
        Planned promotion operations.

    Raises
    ------
    PromotionError
        If promotion cannot be safely planned.

    Notes
    -----
    Atomic promotion is implemented via directory renames and does not copy data.
    """
    previous_root: Optional[Path] = None
    operations: List[str] = []

    if target_root.exists():
        previous_root = _derive_previous_root(target_root, run_id)
        operations.append(f"rename {target_root} -> {previous_root}")

    operations.append(f"rename {stage_root} -> {target_root}")

    _validate_promotion_paths(
        stage_root=stage_root,
        target_root=target_root,
        previous_root=previous_root,
    )

    return PromotionPlan(
        stage_root=stage_root,
        target_root=target_root,
        previous_root=previous_root,
        operations=operations,
    )


def execute_promotion(
    *,
    plan: PromotionPlan,
    dry_run: bool,
    journal: RestoreExecutionJournal | None = None,
) -> PromotionOutcome:
    """
    Execute an atomic promotion according to a promotion plan.

    Parameters
    ----------
    plan : PromotionPlan
        Pre-validated promotion plan.
    dry_run : bool
        If True, no filesystem mutations are performed.
    journal : RestoreExecutionJournal | None
        Optional append-only execution journal.

    Returns
    -------
    PromotionOutcome
        Result of the promotion attempt.

    Raises
    ------
    PromotionError
        If promotion fails mid-operation.

    Notes
    -----
    If promotion fails after partially mutating the filesystem, a
    ``PromotionError`` is raised and the system relies on the preserved
    ``previous_root`` (if any) for recovery.
    """
    if journal is not None:
        journal.append(
            "promotion_planned",
            {
                "stage_root": str(plan.stage_root),
                "target_root": str(plan.target_root),
                "previous_root": str(plan.previous_root) if plan.previous_root else None,
                "operations": list(plan.operations),
                "dry_run": dry_run,
            },
        )

    if dry_run:
        if journal is not None:
            journal.append("promotion_dry_run", {"result": "no_changes"})
        return PromotionOutcome(promoted=False, previous_root=plan.previous_root)

    if journal is not None:
        journal.append("promotion_started", {})

    promoted_previous_root: Path | None = plan.previous_root

    try:
        if plan.previous_root is not None:
            try:
                _move_directory_with_cross_volume_fallback(
                    source_root=plan.target_root,
                    destination_root=plan.previous_root,
                    operation_label="preserve_existing_destination",
                )
            except PromotionError as exc:
                atomic_rename_error = exc.__cause__
                if atomic_rename_error is not None and _is_winerror_32_permission_error(
                    atomic_rename_error
                ):
                    _promote_overwrite_filewise(
                        stage_root=plan.stage_root,
                        target_root=plan.target_root,
                    )
                    promoted_previous_root = None
                else:
                    raise
            else:
                _move_directory_with_cross_volume_fallback(
                    source_root=plan.stage_root,
                    destination_root=plan.target_root,
                    operation_label="promote_stage_root",
                )

        else:
            _move_directory_with_cross_volume_fallback(
                source_root=plan.stage_root,
                destination_root=plan.target_root,
                operation_label="promote_stage_root",
            )

    except Exception as exc:  # noqa: BLE001
        if journal is not None:
            journal.append(
                "promotion_failed",
                {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "stage_exists": plan.stage_root.exists(),
                    "target_exists": plan.target_root.exists(),
                    "previous_exists": plan.previous_root.exists() if plan.previous_root else False,
                },
            )
        if isinstance(exc, PromotionError):
            raise
        raise PromotionError("Atomic promotion failed") from exc

    if journal is not None:
        journal.append(
            "promotion_completed",
            {
                "promoted": True,
                "previous_root": str(promoted_previous_root) if promoted_previous_root else None,
            },
        )

    return PromotionOutcome(promoted=True, previous_root=promoted_previous_root)


def promote_stage_to_destination(
    *,
    stage_root: Path,
    destination_root: Path,
    run_id: str,
    dry_run: bool,
    journal: RestoreExecutionJournal | None = None,
) -> PromotionOutcome:
    """
    Promote a staged restore tree into its final destination atomically.

    Parameters
    ----------
    stage_root:
        Root directory containing staged restore content.
    destination_root:
        Final restore destination directory.
    run_id:
        Unique restore run identifier used to name any preserved previous destination.
    dry_run:
        If True, no filesystem mutations are performed.
    journal:
        Optional execution journal.

    Returns
    -------
    PromotionOutcome
        Result of promotion attempt.

    Raises
    ------
    PromotionError
        If promotion fails mid-operation.

    Notes
    -----
    This is a convenience wrapper around ``plan_promotion`` and
    ``execute_promotion``.
    """
    plan = plan_promotion(stage_root=stage_root, target_root=destination_root, run_id=run_id)
    return execute_promotion(plan=plan, dry_run=dry_run, journal=journal)
