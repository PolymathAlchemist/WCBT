"""
Backup orchestration for WCBT (dry-run planning mode).

This module coordinates:
- profile path resolution (policy + safety gates)
- source validation
- source scanning
- deterministic planning
- deterministic reporting

In dry-run mode, WCBT never deletes. Optionally, it may write a plan artifact
to disk for later viewing (e.g., GUI consumption).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from backup_engine.backup.plan import BackupPlan, attach_scan_issues, build_backup_plan
from backup_engine.backup.render import render_backup_plan_text
from backup_engine.backup.scan import (
    DEFAULT_EXCLUDED_DIRECTORY_NAMES,
    DEFAULT_EXCLUDED_FILE_NAMES,
    ScanRules,
    scan_source_tree,
)
from backup_engine.clock import Clock, SystemClock
from backup_engine.errors import BackupCommitNotImplementedError, WcbtError
from backup_engine.paths_and_safety import resolve_profile_paths, validate_source_path


class PlanArtifactWriteError(WcbtError):
    """
    Raised when a plan artifact cannot be written safely.

    This error is used for failures such as attempting to overwrite an existing
    file without explicit permission, or failing to create required directories.
    """


@dataclass(frozen=True, slots=True)
class BackupRunContext:
    """
    Context for a single backup planning run.

    Attributes
    ----------
    profile_name:
        Profile name used to resolve profile paths.
    source_root:
        Validated source root directory.
    archive_root:
        Planned archive root directory.
    """

    profile_name: str
    source_root: Path
    archive_root: Path


def run_backup(
    *,
    profile_name: str,
    source: Path,
    dry_run: bool,
    data_root: Path | None,
    excluded_directory_names: Iterable[str] | None = None,
    excluded_file_names: Iterable[str] | None = None,
    use_default_excludes: bool = True,
    max_items: int = 100,
    write_plan: bool = False,
    plan_path: Path | None = None,
    overwrite_plan: bool = False,
    clock: Clock | None = None,
) -> None:
    """
    Plan (and in the future, execute) a backup for a given profile.

    Parameters
    ----------
    profile_name:
        Profile name used to select the WCBT profile directory.
    source:
        Source directory to back up.
    dry_run:
        If True, plan and report only. If False, execution is requested.
        Execution is not implemented in v1.
    data_root:
        Optional override for WCBT data root (primarily for tests).
    excluded_directory_names:
        Optional extra directory names to exclude from traversal.
    excluded_file_names:
        Optional extra file names to exclude from traversal.
    use_default_excludes:
        If True, use built-in default excludes in addition to user-provided excludes.
        If False, only the user-provided excludes are used.
    max_items:
        Maximum number of planned operations to print in the plan output.
        Counts always reflect the full plan.
    write_plan:
        If True, write the rendered plan report to disk.
    plan_path:
        Optional override for the plan output file path. If not provided and write_plan
        is True, the default is <archive_root>/plan.txt.
    overwrite_plan:
        If True, allow overwriting an existing plan file. Default is False.
    clock:
        Clock used for deterministic archive identifiers. If not provided,
        SystemClock is used.

    Raises
    ------
    BackupCommitNotImplementedError
        If dry_run is False.
    ValueError
        If max_items is negative.
    PlanArtifactWriteError
        If writing the plan artifact is requested but cannot be done safely.
    """
    if not dry_run:
        raise BackupCommitNotImplementedError(
            "Backup execution is not implemented yet. Use --dry-run."
        )

    if max_items < 0:
        raise ValueError("max_items must be non-negative.")

    run_clock = clock or SystemClock()

    paths = resolve_profile_paths(profile_name, data_root=data_root)
    source_root = validate_source_path(source)

    archive_id = _format_archive_id(run_clock)
    archive_root = paths.archives_root / archive_id

    scan_rules = _build_scan_rules(
        excluded_directory_names=excluded_directory_names,
        excluded_file_names=excluded_file_names,
        use_default_excludes=use_default_excludes,
    )
    scan_result = scan_source_tree(source_root, scan_rules)

    plan = build_backup_plan(entries=scan_result.entries, archive_root=archive_root)
    plan_with_issues = attach_scan_issues(plan, scan_result.issues)

    context = BackupRunContext(
        profile_name=profile_name,
        source_root=source_root,
        archive_root=archive_root,
    )

    report_text = _build_backup_report_text(context, plan_with_issues, max_items=max_items)
    print(report_text)

    if write_plan or plan_path is not None:
        output_path = plan_path or (archive_root / "plan.txt")
        _write_plan_artifact(
            output_path=output_path,
            content=report_text,
            overwrite=overwrite_plan,
        )
        print()
        print(f"Plan written: {output_path}")


def _build_scan_rules(
    *,
    excluded_directory_names: Iterable[str] | None,
    excluded_file_names: Iterable[str] | None,
    use_default_excludes: bool,
) -> ScanRules:
    """
    Construct ScanRules from defaults and user-provided overrides.
    """
    base_directories = DEFAULT_EXCLUDED_DIRECTORY_NAMES if use_default_excludes else frozenset()
    base_files = DEFAULT_EXCLUDED_FILE_NAMES if use_default_excludes else frozenset()

    extra_directories = frozenset(excluded_directory_names or [])
    extra_files = frozenset(excluded_file_names or [])

    return ScanRules(
        excluded_directory_names=base_directories.union(extra_directories),
        excluded_file_names=base_files.union(extra_files),
    )


def _format_archive_id(clock: Clock) -> str:
    """
    Format a deterministic archive identifier using the provided clock.
    """
    current_time = clock.now()
    return current_time.astimezone().strftime("%Y%m%d_%H%M%SZ")


def _build_backup_report_text(
    context: BackupRunContext,
    plan: BackupPlan,
    *,
    max_items: int,
) -> str:
    """
    Build the full human-readable report that is printed and optionally persisted.
    """
    lines: list[str] = []
    lines.append(f"Profile     : {context.profile_name}")
    lines.append(f"Source root : {context.source_root}")
    lines.append(f"Archive root: {context.archive_root}")
    lines.append("")
    lines.append(render_backup_plan_text(plan, max_items=max_items))
    return "\n".join(lines)


def _write_plan_artifact(*, output_path: Path, content: str, overwrite: bool) -> None:
    """
    Write a plan artifact to disk safely.

    Parameters
    ----------
    output_path:
        File path to write.
    content:
        Text content to write.
    overwrite:
        If True, overwrite an existing file. If False, fail if the file exists.

    Raises
    ------
    PlanArtifactWriteError
        If the artifact cannot be written safely.
    """
    try:
        output_path = output_path.expanduser()
        parent = output_path.parent
        parent.mkdir(parents=True, exist_ok=True)

        if overwrite:
            output_path.write_text(content, encoding="utf-8", newline="\n")
            return

        # Safe create: fail if it already exists.
        with output_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    except FileExistsError as exc:
        raise PlanArtifactWriteError(
            f"Plan file already exists (use --overwrite-plan to replace): {output_path}"
        ) from exc
    except OSError as exc:
        raise PlanArtifactWriteError(f"Failed to write plan file: {output_path} ({exc!s})") from exc
