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

import os
import re
import shutil
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
from backup_engine.compression import (
    compute_file_sha256_hex,
)
from backup_engine.errors import BackupExecutionError, WcbtError
from backup_engine.manifest_store import (
    BackupRunArchiveV1,
    BackupRunExecutionV1,
    BackupRunManifestV2,
    RunOperationResultV1,
    write_run_manifest_atomic,
)
from backup_engine.oz0_paths import resolve_oz0_artifact_root
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
    artifact_root: Path
    backup_origin: str = "normal"
    staging_dir: Path | None = None
    root_label: str = "Archive root"


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


_INVALID_FILENAME_CHARACTERS = re.compile(r'[\\/:*?"<>|]+')


def run_backup(
    *,
    profile_name: str,
    source: Path,
    dry_run: bool,
    data_root: Path | None,
    backup_origin: str = "normal",
    job_id: str | None = None,
    job_name: str | None = None,
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
    if backup_origin not in {"normal", "scheduled", "pre_restore"}:
        raise ValueError(f"Unsupported backup_origin: {backup_origin!r}")
    if execute and dry_run:
        raise ValueError("execute=True is not valid in dry-run mode.")
    if compress and not execute:
        raise ValueError(
            "compress=True is only valid when execute=True (post-execute compression)."
        )

    run_clock = clock or SystemClock()

    paths = resolve_profile_paths(profile_name, data_root=data_root)
    source_root = validate_source_path(source)

    run_token = _resolve_run_token(run_clock)
    compression_requested = compress or (compression != "none")
    oz0_root = resolve_oz0_artifact_root(source_root)
    archive_root = oz0_root if dry_run else paths.archives_root / run_token
    if compression_requested:
        artifact_job_name = _resolve_artifact_job_name(
            job_name=job_name,
            job_id=job_id,
            source_root=source_root,
        )
        archive_root = (
            _build_staging_root(
                work_root=paths.work_root,
                artifact_job_name=artifact_job_name,
                run_token=run_token,
            )
            if not dry_run
            else oz0_root
        )
        manifest_output_path = oz0_root / _build_manifest_filename(
            artifact_job_name=artifact_job_name,
            run_token=run_token,
        )
        archive_output_path = oz0_root / _build_archive_filename(
            artifact_job_name=artifact_job_name,
            run_token=run_token,
        )
    else:
        artifact_job_name = None
        manifest_output_path = None
        archive_output_path = None

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
        artifact_root=oz0_root if dry_run or compression_requested else archive_root,
        backup_origin=backup_origin,
        staging_dir=archive_root if compression_requested and not dry_run else None,
        root_label="Artifact root" if compression_requested else "Archive root",
    )

    report_text = _build_backup_report_text(context, plan_with_issues, max_items=max_items)
    print(report_text)

    if dry_run:
        plan_text_path: Path | None = None
        if write_plan or plan_path is not None:
            output_path = plan_path or (context.artifact_root / "plan.txt")
            _write_plan_artifact(
                output_path=output_path,
                content=report_text,
                overwrite=overwrite_plan,
            )
            plan_text_path = output_path
            print()
            print(f"Plan written: {output_path}")

        return BackupRunResult(
            run_id=run_token,
            profile_name=profile_name,
            source_root=source_root,
            archive_root=context.artifact_root,
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
        run_id=run_token,
        force=force,
        break_lock=break_lock,
    ):
        if compression_requested:
            assert oz0_root is not None
            assert manifest_output_path is not None
            assert archive_output_path is not None
            _, manifest_path = _run_compressed_backup(
                plan=plan_with_issues,
                run_root=archive_root,
                run_token=run_token,
                oz0_root=oz0_root,
                manifest_output_path=manifest_output_path,
                archive_output_path=archive_output_path,
                profile_name=profile_name,
                source_root=source_root,
                backup_origin=backup_origin,
                job_id=job_id,
                job_name=job_name,
                clock=run_clock,
            )

            return BackupRunResult(
                run_id=run_token,
                profile_name=profile_name,
                source_root=source_root,
                archive_root=oz0_root,
                dry_run=False,
                report_text=report_text,
                plan_text_path=None,
                manifest_path=manifest_path,
                executed=True,
            )

        materialized = materialize_backup_run(
            plan=plan_with_issues,
            run_root=archive_root,
            run_id=run_token,
            plan_text=report_text,
            profile_name=profile_name,
            source_root=source_root,
            clock=run_clock,
            backup_origin=backup_origin,
            job_id=job_id,
            job_name=job_name,
        )

        print()
        print("Backup run materialized:")
        print(f"  Run directory : {materialized.run_root}")
        print(f"  Plan file     : {materialized.plan_text_path}")
        print(f"  Manifest file : {materialized.manifest_path}")

        if not execute:
            return BackupRunResult(
                run_id=run_token,
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
            required_artifact_paths=(materialized.plan_text_path, materialized.manifest_path),
        )

        compressed_path: Path | None = None
        compression_used = compression
        archive_meta: BackupRunArchiveV1 | None = None

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
                rel = os.path.relpath(compressed_path, start=materialized.manifest_path.parent)
                sha256 = compute_file_sha256_hex(compressed_path)
                size_bytes = compressed_path.stat().st_size

                archive_meta = BackupRunArchiveV1(
                    format=str(fmt.value),
                    relative_path=str(rel).replace("\\", "/"),
                    size_bytes=int(size_bytes),
                    sha256=str(sha256),
                )
                print(f"  Compressed    : {compressed_path}")

        updated_manifest = _build_executed_run_manifest(
            base_manifest=materialized.manifest,
            execution_summary=summary,
            archive=archive_meta,
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
            run_id=run_token,
            profile_name=profile_name,
            source_root=source_root,
            archive_root=archive_root,
            dry_run=False,
            report_text=report_text,
            plan_text_path=materialized.plan_text_path,
            manifest_path=materialized.manifest_path,
            executed=True,
        )


def _run_compressed_backup(
    *,
    plan: BackupPlan,
    run_root: Path,
    run_token: str,
    oz0_root: Path,
    manifest_output_path: Path,
    archive_output_path: Path,
    profile_name: str,
    source_root: Path,
    backup_origin: str,
    job_id: str | None,
    job_name: str | None,
    clock: Clock,
) -> tuple[BackupRunArchiveV1, Path | None]:
    """
    Execute a backup into temporary staging, then emit paired OZ0 artifacts.

    Parameters
    ----------
    plan:
        Backup plan whose destinations already point at ``run_root``.
    run_root:
        Temporary staging directory used only for payload files.
    run_token:
        Shared token for the archive and manifest filenames.
    oz0_root:
        Final directory that receives the archive and manifest outputs.
    manifest_output_path:
        Final manifest path in ``oz0_root``.
    archive_output_path:
        Final archive path in ``oz0_root``.
    profile_name:
        Owning profile name.
    source_root:
        Source root for the run.
    job_id:
        Optional job identifier.
    job_name:
        Optional human-readable job name.
    clock:
        Clock used to stamp the manifest.

    Returns
    -------
    tuple[BackupRunArchiveV1, Path | None]
        Archive metadata and the written manifest path (or ``None`` when manifest
        persistence failed after the archive was created).

    Raises
    ------
    BackupExecutionError
        If file staging or archive creation fails.
    """
    try:
        run_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise BackupExecutionError(f"Staging directory already exists: {run_root}") from exc
    except OSError as exc:
        raise BackupExecutionError(f"Failed to create staging directory: {run_root}") from exc

    summary = execute_copy_plan(
        plan=plan,
        run_root=run_root,
        reserved_paths=(),
        required_artifact_paths=(),
    )

    if summary.status != "success":
        raise BackupExecutionError("Copy execution failed before archive creation.")

    archive_meta = _create_run_archive(
        run_root=run_root,
        archive_output_path=archive_output_path,
    )

    manifest = _build_run_manifest(
        plan=plan,
        run_token=run_token,
        profile_name=profile_name,
        source_root=source_root,
        archive_root=oz0_root,
        manifest_output_path=manifest_output_path,
        backup_origin=backup_origin,
        job_id=job_id,
        job_name=job_name,
        clock=clock,
        execution_summary=summary,
        archive=archive_meta,
    )

    manifest_path: Path | None = None
    try:
        write_run_manifest_atomic(manifest_output_path, manifest)
        manifest_path = manifest_output_path
        print(f"  Manifest      : {manifest_output_path}")
    except Exception as exc:  # noqa: BLE001
        print()
        print(f"ERROR: Failed to write manifest artifact: {manifest_output_path} ({exc!s})")

    try:
        shutil.rmtree(run_root)
    except OSError as exc:
        print()
        print(f"WARNING: Failed to delete staging directory: {run_root} ({exc!s})")

    print()
    print("Copy execution complete:")
    print(f"  Status        : {summary.status}")
    copied_count = sum(1 for r in summary.results if r.outcome.value == "copied")
    print(f"  Copied        : {copied_count}")
    print(f"  Archive       : {archive_output_path}")

    return archive_meta, manifest_path


def _create_run_archive(
    *,
    run_root: Path,
    archive_output_path: Path,
) -> BackupRunArchiveV1:
    """
    Create the final archive artifact for a compressed backup run.

    Notes
    -----
    OZ0 artifact routing uses a fixed ``.OZ0.zip`` contract. The legacy
    compression knobs still decide whether we enter the archive-emission path,
    but the final OZ0 payload is always a zip archive so the filename contract
    remains stable.
    """
    from backup_engine.compression import CompressionFormat, compress_run_directory

    try:
        compressed_path = compress_run_directory(
            run_root=run_root,
            output_path=archive_output_path,
            format=CompressionFormat.ZIP,
        ).archive_path
    except Exception as exc:  # noqa: BLE001
        raise BackupExecutionError(
            f"Failed to create archive artifact: {archive_output_path}"
        ) from exc

    sha256 = compute_file_sha256_hex(compressed_path)
    size_bytes = compressed_path.stat().st_size

    return BackupRunArchiveV1(
        format=str(CompressionFormat.ZIP.value),
        relative_path=compressed_path.name,
        size_bytes=int(size_bytes),
        sha256=str(sha256),
    )


def _build_run_manifest(
    *,
    plan: BackupPlan,
    run_token: str,
    profile_name: str,
    source_root: Path,
    archive_root: Path,
    manifest_output_path: Path,
    backup_origin: str,
    job_id: str | None,
    job_name: str | None,
    clock: Clock,
    execution_summary: BackupExecutionSummary,
    archive: BackupRunArchiveV1 | None,
) -> BackupRunManifestV2:
    """
    Build an in-memory run manifest for final artifact persistence.
    """
    created_at_utc = clock.now().astimezone(timezone.utc)
    operations_payload = _serialize_manifest_operations_without_destination_paths(plan)
    scan_issues_payload = _serialize_scan_issues_for_manifest(plan)

    base_manifest = BackupRunManifestV2(
        schema_version=BackupRunManifestV2.SCHEMA_VERSION,
        run_id=run_token,
        created_at_utc=created_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        archive_root=str(archive_root),
        plan_text_path=str(manifest_output_path.with_suffix(".plan.txt")),
        profile_name=profile_name,
        source_root=str(source_root),
        backup_origin=backup_origin,
        job_id=job_id,
        job_name=job_name,
        operations=list(operations_payload),
        scan_issues=list(scan_issues_payload),
        execution=None,
        archive=None,
    )
    return _build_executed_run_manifest(
        base_manifest=base_manifest,
        execution_summary=execution_summary,
        archive=archive,
        include_destination_paths=False,
    )


def _build_executed_run_manifest(
    *,
    base_manifest: BackupRunManifestV2,
    execution_summary: BackupExecutionSummary,
    archive: BackupRunArchiveV1 | None,
    include_destination_paths: bool = True,
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
                destination_path=r.destination_path if include_destination_paths else None,
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
        backup_origin=base_manifest.backup_origin,
        job_id=base_manifest.job_id,
        job_name=base_manifest.job_name,
        operations=list(base_manifest.operations),
        scan_issues=list(base_manifest.scan_issues),
        execution=execution,
        archive=archive,
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


def _resolve_run_token(clock: Clock) -> str:
    """
    Return the shared token used for all artifacts produced by one run.

    Notes
    -----
    WCBT does not currently expose a separate persisted run-sequence source, so
    the timestamp token remains the fallback and primary token generator.
    """
    return _format_archive_id(clock)


def _resolve_artifact_job_name(
    *, job_name: str | None, job_id: str | None, source_root: Path
) -> str:
    """
    Choose a filesystem-safe name for job-owned artifact filenames.
    """
    for candidate in (job_name, job_id, source_root.name):
        if candidate is None:
            continue
        sanitized = _sanitize_artifact_name(candidate)
        if sanitized:
            return sanitized
    return "job"


def _sanitize_artifact_name(value: str) -> str:
    """
    Sanitize a user-facing artifact name for safe Windows filenames.
    """
    collapsed = " ".join(value.split()).strip().rstrip(". ")
    sanitized = _INVALID_FILENAME_CHARACTERS.sub("_", collapsed)
    return sanitized.strip()


def _build_staging_root(*, work_root: Path, artifact_job_name: str, run_token: str) -> Path:
    """
    Build the temporary staging directory path for a compressed backup run.
    """
    return work_root / "oz0_staging" / f"{artifact_job_name}.{run_token}"


def _build_manifest_filename(*, artifact_job_name: str, run_token: str) -> str:
    """
    Return the final manifest filename for a run.
    """
    return f"{artifact_job_name}.{run_token}.manifest.json"


def _build_archive_filename(*, artifact_job_name: str, run_token: str) -> str:
    """
    Return the default archive filename for a run.
    """
    return f"{artifact_job_name}.{run_token}.OZ0.zip"


def _build_backup_report_text(
    context: BackupRunContext,
    plan: BackupPlan,
    *,
    max_items: int,
) -> str:
    """Build the full human-readable report that is printed and optionally persisted."""
    backup_origin_labels = {
        "normal": "Normal backup",
        "scheduled": "Scheduled backup",
        "pre_restore": "Pre-restore safeguard backup",
    }
    lines: list[str] = []
    lines.append(f"Profile     : {context.profile_name}")
    lines.append(f"Source root : {context.source_root}")
    lines.append(
        f"Backup origin: {backup_origin_labels.get(context.backup_origin, context.backup_origin)}"
    )
    if context.staging_dir is not None:
        lines.append(f"Staging dir : {context.staging_dir}")
        lines.append(f"Artifact root: {context.artifact_root}")
    else:
        lines.append(f"{context.root_label}: {context.artifact_root}")
    lines.append("")
    lines.append(
        render_backup_plan_text(
            plan,
            max_items=max_items,
            root_label="Staging dir" if context.staging_dir is not None else context.root_label,
        )
    )
    return "\n".join(lines)


def _serialize_manifest_operations_without_destination_paths(
    plan: BackupPlan,
) -> list[dict[str, object]]:
    """
    Serialize manifest operations without leaking transient staging filesystem paths.
    """
    operations_payload: list[dict[str, object]] = []
    for operation in plan.operations:
        operations_payload.append(
            {
                "operation_type": operation.operation_type.value,
                "source_path": str(operation.source_path),
                "relative_path": str(operation.relative_path),
                "reason": operation.reason,
            }
        )
    return operations_payload


def _serialize_scan_issues_for_manifest(plan: BackupPlan) -> list[dict[str, object]]:
    """
    Serialize scan issues for inclusion in a run manifest.
    """
    payloads: list[dict[str, object]] = []
    for issue in plan.scan_issues:
        payload: dict[str, object] = {
            "message": str(getattr(issue, "message", "")),
            "issue_type": str(getattr(issue, "issue_type", "")),
        }
        issue_path = getattr(issue, "path", None)
        if issue_path is not None:
            payload["path"] = str(issue_path)
        payloads.append(payload)
    return payloads


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
