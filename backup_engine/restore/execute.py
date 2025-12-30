from __future__ import annotations

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
    """Raised when atomic promotion fails."""


def _derive_previous_root(target_root: Path, run_id: str) -> Path:
    return target_root.with_name(f".wcbt_restore_previous_{target_root.name}_{run_id}")


def _validate_promotion_paths(
    *,
    stage_root: Path,
    target_root: Path,
    previous_root: Optional[Path],
) -> None:
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

    try:
        if plan.previous_root is not None:
            plan.target_root.rename(plan.previous_root)

        plan.stage_root.rename(plan.target_root)

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
        raise PromotionError("Atomic promotion failed") from exc

    if journal is not None:
        journal.append(
            "promotion_completed",
            {
                "promoted": True,
                "previous_root": str(plan.previous_root) if plan.previous_root else None,
            },
        )

    return PromotionOutcome(promoted=True, previous_root=plan.previous_root)


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
    """
    plan = plan_promotion(stage_root=stage_root, target_root=destination_root, run_id=run_id)
    return execute_promotion(plan=plan, dry_run=dry_run, journal=journal)

