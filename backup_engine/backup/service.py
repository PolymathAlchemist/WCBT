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
from datetime import timezone
from pathlib import Path
from typing import Iterable

from backup_engine.backup.execute import BackupExecutionSummary, execute_copy_plan
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
from backup_engine.profile_lock import acquire_profile_lock, build_profile_lock_path


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


@dataclass(frozen=True, slots=True)
class BackupRunResult:
    """
    Summary of a backup planning/materialization run suitable for UI consumption.
    """

    run_id: str
    profile_name: str
    source_root: Path
    archive_root: Path
    dry_run: bool
    report_text: str
    plan_text_path: Path | None
    manifest_path: Path | None
    executed: bool


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
    compress: bool = False,
    compression: str = "none",
    force: bool = False,
    break_lock: bool = False,
) -> BackupRunResult:
    """
    Plan (and optionally materialize and execute) a backup for a given profile.

    Parameters
    ----------
    profile_name:
        Profile name that owns the backup run.
    source:
        Source directory to scan and plan from.
    dry_run:
        If True, plan only and do not create a run directory under archives_root.
    data_root:
        Override WCBT data root (primarily for testing).
    excluded_directory_names:
        Additional directory names to exclude from scanning.
    excluded_file_names:
        Additional file names to exclude from scanning.
    use_default_excludes:
        If True, include the default exclude sets in addition to any user-provided excludes.
    max_items:
        Maximum number of plan entries to render in the human-readable report.
    write_plan:
        If True, write the report text to disk even in dry-run mode.
    plan_path:
        Optional override output path for the plan artifact in dry-run mode.
    overwrite_plan:
        If True, overwrite an existing plan artifact at plan_path/output_path.
    clock:
        Clock override for deterministic testing.
    execute:
        If True, execute the copy plan after materialization.
    force:
        Break a provably stale lock automatically.
    break_lock:
        Break an existing lock even when not provably stale.

    Raises
    ------
    ValueError
        If parameters are invalid (for example max_items < 0 or execute=True with dry_run=True).
    PlanArtifactWriteError
        If a dry-run plan artifact cannot be written safely.
    BackupExecutionError
        If execute=True and copy execution fails.
    WcbtError
        For domain and safety errors.

    Artifacts
    ---------
    When dry_run=True and write_plan=True (or plan_path is provided):
    - Writes a plan text file (default: {archive_root}/plan.txt)

    When dry_run=False:
    - Materializes a run directory under archives_root
    - Writes plan text and a run manifest
    - When execute=True, updates the manifest with per-operation results
    """
    if max_items < 0:
        raise ValueError("max_items must be non-negative.")
    if execute and dry_run:
        raise ValueError("execute=True is not valid in dry-run mode.")
    if compress and not execute:
        raise ValueError(
            "compress=True is only valid when execute=True (post-execute compression)."
        )

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
        plan_text_path: Path | None = None
        if write_plan or plan_path is not None:
            output_path = plan_path or (archive_root / "plan.txt")
            _write_plan_artifact(
                output_path=output_path,
                content=report_text,
                overwrite=overwrite_plan,
            )
            plan_text_path = output_path
            print()
            print(f"Plan written: {output_path}")

        return BackupRunResult(
            run_id=archive_id,
            profile_name=profile_name,
            source_root=source_root,
            archive_root=archive_root,
            dry_run=True,
            report_text=report_text,
            plan_text_path=plan_text_path,
            manifest_path=None,
            executed=False,
        )

    lock_path = build_profile_lock_path(work_root=paths.work_root)
    with acquire_profile_lock(
        lock_path=lock_path,
        profile_name=profile_name,
        command="backup",
        run_id=archive_id,
        force=force,
        break_lock=break_lock,
    ):
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
            return BackupRunResult(
                run_id=archive_id,
                profile_name=profile_name,
                source_root=source_root,
                archive_root=archive_root,
                dry_run=False,
                report_text=report_text,
                plan_text_path=materialized.plan_text_path,
                manifest_path=materialized.manifest_path,
                executed=False,
            )

        summary = execute_copy_plan(
            plan=plan_with_issues,
            run_root=materialized.run_root,
            reserved_paths=(materialized.plan_text_path, materialized.manifest_path),
        )

        compressed_path: Path | None = None
        compression_used = compression

        if compress or (compression_used != "none"):
            from backup_engine.compression import CompressionFormat, compress_run_directory

            requested = compression_used
            if compress and requested == "none":
                requested = "tar.zst"

            fmt = CompressionFormat(requested)

            # Derived artifact lives next to the run directory
            if fmt is CompressionFormat.ZIP:
                out = materialized.run_root.with_suffix(".zip")
                compressed_path = compress_run_directory(
                    run_root=materialized.run_root,
                    output_path=out,
                    format=CompressionFormat.ZIP,
                ).archive_path
            elif fmt is CompressionFormat.TAR_ZST:
                out = materialized.run_root.with_suffix(".tar.zst")
                try:
                    compressed_path = compress_run_directory(
                        run_root=materialized.run_root,
                        output_path=out,
                        format=CompressionFormat.TAR_ZST,
                    ).archive_path
                except RuntimeError:
                    # Optional fallback to zip when tar.zst is unavailable
                    fallback = materialized.run_root.with_suffix(".zip")
                    compressed_path = compress_run_directory(
                        run_root=materialized.run_root,
                        output_path=fallback,
                        format=CompressionFormat.ZIP,
                    ).archive_path
                    print()
                    print("NOTE: tar.zst unavailable; fell back to zip.")
            else:
                compressed_path = None

            if compressed_path is not None:
                print(f"  Compressed    : {compressed_path}")

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
            raise BackupExecutionError(
                "Copy execution failed. See manifest.json for per-operation results."
            )

        return BackupRunResult(
            run_id=archive_id,
            profile_name=profile_name,
            source_root=source_root,
            archive_root=archive_root,
            dry_run=False,
            report_text=report_text,
            plan_text_path=materialized.plan_text_path,
            manifest_path=materialized.manifest_path,
            executed=True,
        )


def _build_executed_run_manifest(
    *,
    base_manifest: BackupRunManifestV2,
    execution_summary: BackupExecutionSummary,
) -> BackupRunManifestV2:
    """
    Create a new run manifest including execution results.

    Parameters
    ----------
    base_manifest:
        Materialized run manifest without execution results.
    execution_summary:
        Execution summary produced by the copy executor.

    Returns
    -------
    BackupRunManifestV2
        New manifest with an execution payload attached.
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
    """
    Write a plan artifact to disk safely.

    Parameters
    ----------
    output_path:
        Target output path for the plan text.
    content:
        Plan text content to write.
    overwrite:
        If True, overwrite output_path when it exists.

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

        with output_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    except FileExistsError as exc:
        raise PlanArtifactWriteError(
            f"Plan file already exists (use --overwrite-plan to replace): {output_path}"
        ) from exc
    except OSError as exc:
        raise PlanArtifactWriteError(f"Failed to write plan file: {output_path} ({exc!s})") from exc
