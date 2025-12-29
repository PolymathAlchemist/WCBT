"""
Copy execution for WCBT backup runs.

This module performs the first "real" backup execution phase: copying planned
files into a materialized run directory.

Safety posture
--------------
- Executes only COPY_FILE_TO_ARCHIVE operations.
- Never deletes anything.
- Never overwrites existing destination files.
- Fails fast on invariant violations and records outcomes deterministically.
- Requires the run directory and its artifacts (plan.txt, manifest.json) to
  already exist before any copy occurs.

Design notes
------------
- This module performs filesystem writes (copy + mkdir), but only within the
  already-materialized run directory.
- It does not depend on the CLI.
- Deterministic behavior: outcomes preserve plan order exactly.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable

from backup_engine.backup.plan import BackupPlan, PlannedOperation, PlannedOperationType
from backup_engine.errors import BackupExecutionError, BackupInvariantViolationError


class OperationOutcome(str, Enum):
    """
    Outcome values for execution results.

    These values are persisted into the run manifest. Treat them as a stable
    external contract.
    """

    COPIED = "copied"
    SKIPPED_NON_COPY_OPERATION = "skipped_non_copy_operation"
    FAILED_INVARIANT = "failed_invariant"
    FAILED_IO = "failed_io"


@dataclass(frozen=True, slots=True)
class OperationExecutionResult:
    """
    Result of executing (or intentionally skipping) a single planned operation.

    Attributes
    ----------
    operation_index:
        Index of the operation in plan.operations.
    operation_type:
        Operation type string value.
    relative_path:
        Relative path for the operation (string form).
    source_path:
        Absolute source path (string form).
    destination_path:
        Absolute destination path (string form).
    outcome:
        Outcome of the operation.
    message:
        Human-readable detail for the outcome (safe to print/log).
    """

    operation_index: int
    operation_type: str
    relative_path: str
    source_path: str
    destination_path: str
    outcome: OperationOutcome
    message: str


@dataclass(frozen=True, slots=True)
class BackupExecutionSummary:
    """
    Summary of executing a backup plan.

    Attributes
    ----------
    status:
        Overall status string: "success" or "failed".
    results:
        Per-operation results in deterministic order (same as plan.operations).
    """

    status: str
    results: list[OperationExecutionResult]


def execute_copy_plan(
    *,
    plan: BackupPlan,
    run_root: Path,
    reserved_paths: Iterable[Path],
) -> BackupExecutionSummary:
    """
    Execute copy operations for a materialized backup run.

    Parameters
    ----------
    plan:
        Backup plan to execute.
    run_root:
        Materialized run directory (plan destinations are expected to be under this root).
    reserved_paths:
        Paths that must never be overwritten (e.g., plan.txt, manifest.json).

    Returns
    -------
    BackupExecutionSummary
        Execution results in deterministic order.

    Raises
    ------
    BackupInvariantViolationError
        If required artifacts are missing or if a planned destination is unsafe.
    BackupExecutionError
        If an unrecoverable execution failure occurs.
    """
    _assert_materialized_run_invariants(run_root=run_root)

    reserved_set = {p.resolve() for p in reserved_paths}
    run_root_resolved = run_root.resolve()

    results: list[OperationExecutionResult] = []

    for index, operation in enumerate(plan.operations):
        result = _execute_single_operation(
            operation_index=index,
            operation=operation,
            run_root=run_root_resolved,
            reserved_paths=reserved_set,
        )
        results.append(result)

        if result.outcome in {OperationOutcome.FAILED_INVARIANT, OperationOutcome.FAILED_IO}:
            # Fail fast: stop on first failure, but return deterministic partial results.
            return BackupExecutionSummary(status="failed", results=results)

    return BackupExecutionSummary(status="success", results=results)


def _assert_materialized_run_invariants(*, run_root: Path) -> None:
    """
    Ensure the run directory and required artifacts exist before copying.

    Raises
    ------
    BackupInvariantViolationError
        If invariants are not satisfied.
    """
    if not run_root.exists():
        raise BackupInvariantViolationError(f"Run directory does not exist: {run_root}")
    if not run_root.is_dir():
        raise BackupInvariantViolationError(f"Run root is not a directory: {run_root}")

    plan_path = run_root / "plan.txt"
    manifest_path = run_root / "manifest.json"

    if not plan_path.is_file():
        raise BackupInvariantViolationError(f"Missing required artifact: {plan_path}")
    if not manifest_path.is_file():
        raise BackupInvariantViolationError(f"Missing required artifact: {manifest_path}")


def _execute_single_operation(
    *,
    operation_index: int,
    operation: PlannedOperation,
    run_root: Path,
    reserved_paths: set[Path],
) -> OperationExecutionResult:
    """
    Execute a single planned operation.

    Notes
    -----
    - Non-copy operations are skipped (recorded explicitly).
    - Copy operations are executed with strict safety checks.
    """
    if operation.operation_type is not PlannedOperationType.COPY_FILE_TO_ARCHIVE:
        return OperationExecutionResult(
            operation_index=operation_index,
            operation_type=operation.operation_type.value,
            relative_path=str(operation.relative_path),
            source_path=str(operation.source_path),
            destination_path=str(operation.destination_path),
            outcome=OperationOutcome.SKIPPED_NON_COPY_OPERATION,
            message="Operation type is not executable in copy-only milestone.",
        )

    destination_path = Path(operation.destination_path).resolve()
    source_path = Path(operation.source_path).resolve()

    try:
        _assert_destination_is_safe(
            run_root=run_root,
            destination_path=destination_path,
            reserved_paths=reserved_paths,
        )
        _copy_file_strict(source_path=source_path, destination_path=destination_path)
        return OperationExecutionResult(
            operation_index=operation_index,
            operation_type=operation.operation_type.value,
            relative_path=str(operation.relative_path),
            source_path=str(source_path),
            destination_path=str(destination_path),
            outcome=OperationOutcome.COPIED,
            message="Copied successfully.",
        )
    except BackupInvariantViolationError as exc:
        return OperationExecutionResult(
            operation_index=operation_index,
            operation_type=operation.operation_type.value,
            relative_path=str(operation.relative_path),
            source_path=str(source_path),
            destination_path=str(destination_path),
            outcome=OperationOutcome.FAILED_INVARIANT,
            message=str(exc),
        )
    except OSError as exc:
        return OperationExecutionResult(
            operation_index=operation_index,
            operation_type=operation.operation_type.value,
            relative_path=str(operation.relative_path),
            source_path=str(source_path),
            destination_path=str(destination_path),
            outcome=OperationOutcome.FAILED_IO,
            message=f"Copy failed: {exc!s}",
        )
    except BackupExecutionError as exc:
        return OperationExecutionResult(
            operation_index=operation_index,
            operation_type=operation.operation_type.value,
            relative_path=str(operation.relative_path),
            source_path=str(source_path),
            destination_path=str(destination_path),
            outcome=OperationOutcome.FAILED_INVARIANT,
            message=str(exc),
        )


def _assert_destination_is_safe(
    *,
    run_root: Path,
    destination_path: Path,
    reserved_paths: set[Path],
) -> None:
    """
    Validate that the destination path is safe to write.

    Safety rules
    ------------
    - Destination must be within run_root.
    - Destination must not target reserved artifacts.
    - Destination file must not already exist.
    """
    try:
        destination_path.relative_to(run_root)
    except ValueError as exc:
        raise BackupInvariantViolationError(
            f"Planned destination escapes run root: {destination_path} (run root: {run_root})"
        ) from exc

    if destination_path in reserved_paths:
        raise BackupInvariantViolationError(f"Planned destination targets a reserved artifact: {destination_path}")

    if destination_path.exists():
        raise BackupInvariantViolationError(f"Destination already exists (will not overwrite): {destination_path}")


def _copy_file_strict(*, source_path: Path, destination_path: Path) -> None:
    """
    Copy a file with strict safety behavior.

    Parameters
    ----------
    source_path:
        Absolute source file path.
    destination_path:
        Absolute destination file path.

    Raises
    ------
    BackupInvariantViolationError
        If the source path is missing or is not a regular file, or if the source is a symlink.
    OSError
        If filesystem operations fail.
    """
    if not source_path.exists():
        raise BackupInvariantViolationError(f"Source file missing at execution time: {source_path}")
    if not source_path.is_file():
        raise BackupInvariantViolationError(f"Source path is not a file: {source_path}")
    if source_path.is_symlink():
        raise BackupInvariantViolationError(f"Refusing to copy symlink/reparse point: {source_path}")

    destination_parent = destination_path.parent
    destination_parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(source_path, destination_path)
