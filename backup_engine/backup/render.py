"""
Rendering for backup planning output.

This module renders a BackupPlan to deterministic, human-readable text.
"""

from __future__ import annotations

from backup_engine.backup.plan import BackupPlan, PlannedOperationType

_OPERATION_ORDER: tuple[PlannedOperationType, ...] = (
    PlannedOperationType.COPY_FILE_TO_ARCHIVE,
    PlannedOperationType.SKIP_UNSAFE_PATH,
)


def render_backup_plan_text(plan: BackupPlan, *, max_items: int) -> str:
    """
    Render a backup plan as deterministic plain text.

    Parameters
    ----------
    plan:
        The backup plan to render.
    max_items:
        Maximum number of planned operations to list. Counts always reflect the full plan.
        Must be non-negative.

    Returns
    -------
    str
        A deterministic text representation of the plan.

    Raises
    ------
    ValueError
        If max_items is negative.
    """
    if max_items < 0:
        raise ValueError("max_items must be non-negative.")

    lines: list[str] = []
    lines.append("Backup plan")
    lines.append(f"Archive root: {plan.archive_root}")
    lines.append("")

    counts = _count_operations(plan)
    for operation_type in _OPERATION_ORDER:
        lines.append(f"{operation_type.value}: {counts.get(operation_type, 0)}")

    if plan.scan_issues:
        lines.append("")
        lines.append(f"scan_issues: {len(plan.scan_issues)}")

    lines.append("")

    total_operations = len(plan.operations)
    shown_operations = plan.operations[:max_items] if max_items else []

    for operation in shown_operations:
        lines.append(f"{operation.operation_type.value}: {operation.relative_path}")

    if max_items < total_operations:
        remaining = total_operations - max_items
        lines.append(f"... ({remaining} more not shown)")

    if plan.scan_issues:
        lines.append("")
        lines.append("Scan issues:")
        for issue in plan.scan_issues:
            lines.append(f"- {issue.path}: {issue.message}")

    return "\n".join(lines)


def _count_operations(plan: BackupPlan) -> dict[PlannedOperationType, int]:
    """
    Count operations by type.

    Parameters
    ----------
    plan:
        Plan to count.

    Returns
    -------
    dict[PlannedOperationType, int]
        Counts per operation type.
    """
    counts: dict[PlannedOperationType, int] = {}
    for operation in plan.operations:
        counts[operation.operation_type] = counts.get(operation.operation_type, 0) + 1
    return counts
