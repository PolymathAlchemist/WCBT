"""
Backup orchestration for WCBT.

This module coordinates:
- profile path resolution (policy + safety gates)
- source validation
- source scanning
- deterministic planning
- deterministic reporting
- optional run materialization (mkdir + artifacts)
- optional copy execution (copy-only milestone)

Safety posture (v1)
-------------------
- Default behavior is plan-only (no filesystem writes, no deletion).
- Materialize mode creates a run directory and writes plan.txt + manifest.json.
- Execute mode copies planned files into the run directory:
  - copy-only (no deletion, no compression),
  - never overwrites,
  - fail-fast on invariant violations,
  - records outcomes deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from backup_engine.backup.execute import execute_copy_plan
from backup_engine.backup.materialize import materialize_backup_run
from backup_engine.backup.plan import BackupPlan, attach_scan_issues, build_backup_plan
from backup_engine.backup.render import render_backup_plan_text
from backup_engine.backup.scan import (
    DEFAULT_EXCLUDED_DIRECTORY_NAMES,
    DEFAULT_EXCLUDED_FILE_NAMES,
    ScanRules,
    scan_source_tree,
)
from backup_engine.clock import Clock, SystemClock
from backup_engine.errors import BackupExecutionError, WcbtError
from backup_engine.manifest_store import (
    BackupRunExecutionV1,
    BackupRunManifestV2,
    RunOperationResultV1,
    write_run_manifest_atomic,
)
from backup_engine.paths_and_safety import resolve_profile_paths, validate_source_path
from datetime import timezone

class PlanArtifactWriteError(WcbtError):
    """
    Raised when a plan artifact cannot be written safely.
    """


@dataclass(frozen=True, slots=True)
class BackupRunContext:
    """
    Context for a single backup planning run.
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
    execute: bool = False,
) -> None:
    """
    Plan (and optionally materialize and execute) a backup for a given profile.
    """
    if max_items < 0:
        raise ValueError("max_items must be non-negative.")
    if execute and dry_run:
        raise ValueError("execute=True is not valid in dry-run mode.")

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

    if dry_run:
        if write_plan or plan_path is not None:
            output_path = plan_path or (archive_root / "plan.txt")
            _write_plan_artifact(
                output_path=output_path,
                content=report_text,
                overwrite=overwrite_plan,
            )
            print()
            print(f"Plan written: {output_path}")
        return

    materialized = materialize_backup_run(
        plan=plan_with_issues,
        run_root=archive_root,
        run_id=archive_id,
        plan_text=report_text,
        profile_name=profile_name,
        source_root=source_root,
        clock=run_clock,
    )

    print()
    print("Backup run materialized:")
    print(f"  Run directory : {materialized.run_root}")
    print(f"  Plan file     : {materialized.plan_text_path}")
    print(f"  Manifest file : {materialized.manifest_path}")

    if not execute:
        return

    summary = execute_copy_plan(
        plan=plan_with_issues,
        run_root=materialized.run_root,
        reserved_paths=(materialized.plan_text_path, materialized.manifest_path),
    )

    updated_manifest = _build_executed_run_manifest(
        base_manifest=materialized.manifest,
        execution_summary=summary,
    )
    write_run_manifest_atomic(materialized.manifest_path, updated_manifest)

    print()
    print("Copy execution complete:")
    print(f"  Status        : {summary.status}")
    copied_count = sum(1 for r in summary.results if r.outcome.value == "copied")
    print(f"  Copied        : {copied_count}")
    print(f"  Results written: {materialized.manifest_path}")

    if summary.status != "success":
        raise BackupExecutionError("Copy execution failed. See manifest.json for per-operation results.")


def _build_executed_run_manifest(
    *,
    base_manifest: BackupRunManifestV2,
    execution_summary,
) -> BackupRunManifestV2:
    """
    Create a new run manifest including execution results.
    """
    results: list[RunOperationResultV1] = []
    for r in execution_summary.results:
        results.append(
            RunOperationResultV1(
                operation_index=r.operation_index,
                operation_type=r.operation_type,
                relative_path=r.relative_path,
                source_path=r.source_path,
                destination_path=r.destination_path,
                outcome=r.outcome.value,
                message=r.message,
            )
        )

    execution = BackupRunExecutionV1(status=execution_summary.status, results=results)

    return BackupRunManifestV2(
        schema_version=base_manifest.schema_version,
        run_id=base_manifest.run_id,
        created_at_utc=base_manifest.created_at_utc,
        archive_root=base_manifest.archive_root,
        plan_text_path=base_manifest.plan_text_path,
        profile_name=base_manifest.profile_name,
        source_root=base_manifest.source_root,
        operations=list(base_manifest.operations),
        scan_issues=list(base_manifest.scan_issues),
        execution=execution,
    )


def _build_scan_rules(
    *,
    excluded_directory_names: Iterable[str] | None,
    excluded_file_names: Iterable[str] | None,
    use_default_excludes: bool,
) -> ScanRules:
    """Construct ScanRules from defaults and user-provided overrides."""
    base_directories = DEFAULT_EXCLUDED_DIRECTORY_NAMES if use_default_excludes else frozenset()
    base_files = DEFAULT_EXCLUDED_FILE_NAMES if use_default_excludes else frozenset()

    extra_directories = frozenset(excluded_directory_names or [])
    extra_files = frozenset(excluded_file_names or [])

    return ScanRules(
        excluded_directory_names=base_directories.union(extra_directories),
        excluded_file_names=base_files.union(extra_files),
    )


def _format_archive_id(clock: Clock) -> str:
    current_time = clock.now()
    utc_time = current_time.astimezone(timezone.utc)
    return utc_time.strftime("%Y%m%d_%H%M%SZ")


def _build_backup_report_text(
    context: BackupRunContext,
    plan: BackupPlan,
    *,
    max_items: int,
) -> str:
    """Build the full human-readable report that is printed and optionally persisted."""
    lines: list[str] = []
    lines.append(f"Profile     : {context.profile_name}")
    lines.append(f"Source root : {context.source_root}")
    lines.append(f"Archive root: {context.archive_root}")
    lines.append("")
    lines.append(render_backup_plan_text(plan, max_items=max_items))
    return "\n".join(lines)


def _write_plan_artifact(*, output_path: Path, content: str, overwrite: bool) -> None:
    """Write a plan artifact to disk safely."""
    try:
        output_path = output_path.expanduser()
        parent = output_path.parent
        parent.mkdir(parents=True, exist_ok=True)

        if overwrite:
            output_path.write_text(content, encoding="utf-8", newline="\n")
            return

        with output_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    except FileExistsError as exc:
        raise PlanArtifactWriteError(
            f"Plan file already exists (use --overwrite-plan to replace): {output_path}"
        ) from exc
    except OSError as exc:
        raise PlanArtifactWriteError(f"Failed to write plan file: {output_path} ({exc!s})") from exc
