from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from backup_engine.backup.service import BackupRunResult, run_backup
from backup_engine.manifest_store import read_manifest_json, write_json_atomic
from backup_engine.profile_store.sqlite_store import open_profile_store

from .data_models import RestoreIntent
from .errors import RestoreArtifactError, RestoreConflictError, RestoreManifestError
from .execute import promote_stage_to_destination
from .journal import Clock, RestoreExecutionJournal
from .materialize import materialize_restore_candidates
from .plan import build_restore_plan, parse_restore_mode, parse_restore_verification
from .stage import build_restore_stage
from .verify import verify_restore_stage

__all__ = [
    "run_restore",
    "RestoreIntent",
]


def _build_extract_root(destination_root: Path, stamp: str) -> Path:
    """
    Build a temporary extraction root outside the destination tree.
    """
    return destination_root.with_name(f"{destination_root.name}.wcbt_restore_extract") / stamp


def _resolve_manifest_input(
    *,
    manifest_input: Path,
    destination_root: Path,
    clock: Clock,
) -> tuple[Path, Path | None]:
    """
    Resolve a restore input into a concrete manifest.json path.

    Parameters
    ----------
    manifest_input:
        Path provided by the caller. May be:
        - a manifest.json file
        - a run directory containing manifest.json
        - a compressed archive (.zip or .tar.zst) containing a run directory
    destination_root:
        Destination root used to place extraction staging (when needed).

    Returns
    -------
    tuple[pathlib.Path, pathlib.Path | None]
        (resolved_manifest_path, extracted_root_or_none)

    Raises
    ------
    ValueError
        If the input cannot be resolved to a manifest.json.
    RuntimeError
        If extraction requires optional dependencies (tar.zst without zstandard).
    """
    from backup_engine.compression import extract_archive

    manifest_path = manifest_input.expanduser()

    if manifest_path.is_dir():
        candidate = manifest_path / "manifest.json"
        if candidate.is_file():
            return candidate, None
        raise ValueError(f"Run directory does not contain manifest.json: {manifest_path}")

    lower = manifest_path.name.lower()
    if lower.endswith(".zip") or lower.endswith(".tar.zst") or lower.endswith(".tarzst"):
        stamp = clock.now().astimezone(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
        extract_root = _build_extract_root(destination_root, stamp)
        extract_archive(archive_path=manifest_path, destination_dir=extract_root)

        # Expect exactly one top-level directory (run_id)
        children = [c for c in extract_root.iterdir() if c.is_dir()]
        if len(children) != 1:
            raise ValueError(
                f"Expected archive to contain exactly one run directory; found {len(children)} at {extract_root}"
            )
        manifest_path = children[0] / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError(f"Extracted run directory missing manifest.json: {children[0]}")
        return manifest_path, extract_root

    # Otherwise must be a manifest.json file
    if manifest_path.is_file():
        # If this is a run manifest that points at a derived archive, prefer restoring from that archive.
        try:
            payload = read_manifest_json(manifest_path)
        except Exception:
            return manifest_path, None

        archive = payload.get("archive")
        if isinstance(archive, Mapping):
            rel = archive.get("relative_path")
            if isinstance(rel, str) and rel.strip():
                archive_path = (manifest_path.parent / rel).resolve()
                if archive_path.exists() and archive_path.is_file():
                    stamp = clock.now().astimezone(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
                    extract_root = _build_extract_root(destination_root, stamp)
                    extract_archive(archive_path=archive_path, destination_dir=extract_root)

                    children = [c for c in extract_root.iterdir() if c.is_dir()]
                    if len(children) != 1:
                        raise ValueError(
                            f"Expected archive to contain exactly one run directory; found {len(children)} at {extract_root}"
                        )
                    extracted_run_root = children[0]
                    extracted_manifest_path = extracted_run_root / "manifest.json"
                    if extracted_manifest_path.is_file():
                        return extracted_manifest_path, extract_root

                    extracted_payload = dict(payload)
                    extracted_payload["archive_root"] = str(extracted_run_root)
                    write_json_atomic(extracted_manifest_path, extracted_payload)
                    return extracted_manifest_path, extract_root

                raise RestoreManifestError(
                    "Manifest references archive metadata, but the archive could not be found. "
                    f"manifest={manifest_path} archive={archive_path}. "
                    "Restore cannot proceed from this manifest without the archive."
                )

        return manifest_path, None

    raise ValueError(f"Invalid manifest input: {manifest_input}")


def _restore_artifacts_root(destination_root: Path, run_id: str) -> Path:
    """
    Return the root directory for restore artifacts.

    Parameters
    ----------
    destination_root:
        Destination root for the restore.
    run_id:
        Restore run identifier.

    Returns
    -------
    pathlib.Path
        Path to the restore artifacts root directory.
    """
    return destination_root / ".wcbt_restore" / run_id


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    Write a JSON artifact to disk using atomic replacement.

    Parameters
    ----------
    path:
        Target output path.
    payload:
        JSON-serializable payload.

    Raises
    ------
    RestoreArtifactError
        If the artifact cannot be written.
    """
    try:
        write_json_atomic(path, payload)
    except Exception as exc:  # noqa: BLE001
        raise RestoreArtifactError(f"Failed to write JSON artifact: {path}") from exc


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """
    Write a deterministic JSONL artifact to disk.

    Parameters
    ----------
    path:
        Target output path.
    rows:
        Row payloads to serialize (one JSON object per line).

    Raises
    ------
    RestoreArtifactError
        If the artifact cannot be written.

    Notes
    -----
    Rows are serialized with sorted keys and LF newlines to keep artifacts stable
    across platforms and runs.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False))
                handle.write("\n")
    except OSError as exc:
        raise RestoreArtifactError(f"Failed to write JSONL artifact: {path}") from exc


def _write_restore_summary(
    *,
    artifacts_root: Path,
    run_id: str,
    manifest_path: Path,
    destination_root: Path,
    mode: str,
    verify: str,
    dry_run: bool,
    result: str,
    counts: dict[str, int],
) -> None:
    """
    Write the restore summary JSON artifact.

    Parameters
    ----------
    artifacts_root:
        Root directory where restore artifacts are written.
    run_id:
        Restore run identifier.
    manifest_path:
        Path to the source run manifest.
    destination_root:
        Destination root directory for the restore.
    mode:
        Restore mode string (for example, 'add-only' or 'overwrite').
    verify:
        Verification mode string (for example, 'none' or 'size').
    dry_run:
        If True, no files are copied and no promotion is performed.
    result:
        Result classification string ('ok', 'conflict', or 'error').
    counts:
        Deterministic count summary (planned, conflicts, staged_files, failed_files, verified_files).

    Raises
    ------
    RestoreArtifactError
        If the summary cannot be written.
    """
    _write_json(
        artifacts_root / "restore_summary.json",
        {
            "run_id": run_id,
            "manifest_path": str(manifest_path),
            "destination_root": str(destination_root),
            "mode": mode,
            "verify": verify,
            "dry_run": dry_run,
            "result": result,  # "ok" | "conflict" | "error"
            "counts": counts,
        },
    )


@dataclass(frozen=True, slots=True)
class RestoreRunResult:
    """
    Summary of a restore run suitable for UI consumption.
    """

    run_id: str
    manifest_path: Path
    destination_root: Path
    dry_run: bool
    mode: str
    verify: str
    artifacts_root: Path
    stage_root: Path
    summary_path: Path


@dataclass(frozen=True)
class SystemClock(Clock):
    """
    Wall-clock implementation for production runs.

    Returns UTC timestamps to support deterministic journaling and artifact writing
    across machines.
    """

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def _resolve_pre_restore_backup_job(
    *,
    profile_name: str,
    data_root: Path | None,
    destination_root: Path,
    manifest_job_id: str | None,
) -> tuple[str | None, str | None, str]:
    """
    Resolve the current live job definition for a pre-restore backup when possible.

    Parameters
    ----------
    profile_name:
        Profile that should own the pre-restore backup.
    data_root:
        Optional data root for the profile store.
    destination_root:
        Live restore destination that may match a current job binding.
    manifest_job_id:
        Historical job identifier from the restore manifest, if present.

    Returns
    -------
    tuple[str | None, str | None, str]
        ``(job_id, job_name, compression)`` for the current live job when found.

    Notes
    -----
    The restore manifest remains historical input. Pre-restore backup must prefer
    the current live job binding that currently owns ``destination_root``.
    """
    destination_resolved = destination_root.resolve()
    store = open_profile_store(profile_name=profile_name, data_root=data_root)

    fallback_match: tuple[str, str, str] | None = None
    for summary in store.list_jobs():
        binding = store.load_job_binding(summary.job_id)
        if not binding.source_root.strip():
            continue

        try:
            binding_root = Path(binding.source_root).expanduser().resolve()
        except OSError:
            continue

        if binding_root != destination_resolved:
            continue

        compression = store.load_template_compression(binding.job_id)
        resolved_match = (binding.job_id, binding.job_name, compression)
        if manifest_job_id is not None and binding.job_id == manifest_job_id:
            return resolved_match
        if fallback_match is None:
            fallback_match = resolved_match

    if fallback_match is not None:
        return fallback_match

    return None, None, "zip"


def _run_pre_restore_backup(
    *,
    run_payload: Mapping[str, Any],
    destination_root: Path,
    data_root: Path | None,
    clock: Clock,
    journal: RestoreExecutionJournal,
) -> BackupRunResult | None:
    """
    Execute a pre-restore backup of the current live destination when applicable.

    Parameters
    ----------
    run_payload:
        Parsed restore manifest payload.
    destination_root:
        Current live restore destination.
    data_root:
        Optional data root for backup/profile-store resolution.
    clock:
        Clock shared with the restore run for deterministic testing.
    journal:
        Restore execution journal used for audit events.

    Returns
    -------
    BackupRunResult | None
        Backup result when a live destination was protected, otherwise ``None``.
    """
    if not destination_root.exists() or not destination_root.is_dir():
        journal.append(
            "pre_restore_backup_skipped",
            {
                "reason": "destination_missing",
                "destination_root": str(destination_root),
            },
        )
        return None

    profile_name_raw = run_payload.get("profile_name")
    profile_name = str(profile_name_raw).strip() if profile_name_raw is not None else ""
    if not profile_name:
        profile_name = "default"

    manifest_job_id = run_payload.get("job_id")
    job_id = str(manifest_job_id) if isinstance(manifest_job_id, str) and manifest_job_id else None

    resolved_job_id, resolved_job_name, compression = _resolve_pre_restore_backup_job(
        profile_name=profile_name,
        data_root=data_root,
        destination_root=destination_root,
        manifest_job_id=job_id,
    )

    journal.append(
        "pre_restore_backup_started",
        {
            "initiated_by": "restore",
            "destination_root": str(destination_root),
            "job_id": resolved_job_id,
            "job_name": resolved_job_name,
            "compression": compression,
        },
    )

    result = run_backup(
        profile_name=profile_name,
        source=destination_root,
        dry_run=False,
        data_root=data_root,
        backup_origin="pre_restore",
        execute=True,
        compression=compression,
        job_id=resolved_job_id,
        job_name=resolved_job_name if resolved_job_name is not None else destination_root.name,
        clock=clock,
    )

    journal.append(
        "pre_restore_backup_completed",
        {
            "initiated_by": "restore",
            "backup_run_id": result.run_id,
            "archive_root": str(result.archive_root),
            "manifest_path": str(result.manifest_path)
            if result.manifest_path is not None
            else None,
        },
    )
    return result


def run_restore(
    *,
    manifest_path: Path,
    destination_root: Path,
    mode: str,
    verify: str,
    dry_run: bool,
    data_root: Path | None = None,
    clock: Clock | None = None,
) -> RestoreRunResult:
    """
    Plan, stage, optionally verify, and optionally promote a restore.

    This function performs the restore pipeline:
    - validate inputs and parse mode/verification options
    - build a deterministic restore plan
    - materialize candidate actions
    - write artifact-first plan outputs
    - build a staged restore tree
    - optionally verify the staged tree
    - if not dry-run, promote the staged tree into destination_root

    Parameters
    ----------
    manifest_path:
        Path to a run manifest.json (schema_version 'wcbt_run_manifest_v2').
    destination_root:
        Destination root directory for the restore.
    mode:
        Restore mode string (for example, 'add-only' or 'overwrite').
    verify:
        Verification mode string (for example, 'none' or 'size').
    dry_run:
        If True, write artifacts and journal, but do not copy files into the stage and do not promote.
    data_root:
        Reserved for future profile-oriented restore flows. Currently unused.
    clock:
        Injectable clock used for deterministic journaling.

    Returns
    -------
    None

    Raises
    ------
    RestoreManifestError
        If the run manifest is missing required fields or has an unsupported schema version.
    RestoreArtifactError
        If an artifact cannot be written.
    RestoreConflictError
        If conflicts are detected in add-only mode.
    RestoreManifestError
        If required manifest structure is invalid.
    Exception
        Re-raises unexpected exceptions after writing a best-effort error summary.

    Artifacts
    ---------
    Artifacts are written under either:

    - Dry-run: `{destination_root}/.wcbt_restore/{run_id}/`
    - Non-dry-run: `{stage_root}/.wcbt_restore/{run_id}/` (so artifacts survive atomic promotion)

    The following artifacts may be written:
    - `restore_plan.json`
    - `restore_candidates.jsonl`
    - `restore_conflicts.jsonl` (add-only conflicts only)
    - `execution_journal.jsonl`
    - `restore_summary.json`
    """
    _ = data_root

    clock_to_use = clock if clock is not None else SystemClock()

    resolved_manifest_path, _extracted_root = _resolve_manifest_input(
        manifest_input=manifest_path,
        destination_root=destination_root,
        clock=clock_to_use,
    )
    run_payload = read_manifest_json(resolved_manifest_path)
    manifest_path = resolved_manifest_path
    schema_version = run_payload.get("schema_version")
    if schema_version != "wcbt_run_manifest_v2":
        raise RestoreManifestError(f"Unsupported manifest schema_version: {schema_version!r}")

    operations = run_payload.get("operations")
    if not isinstance(operations, list):
        raise RestoreManifestError("Run manifest must contain a list 'operations'.")

    intent = RestoreIntent(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode=parse_restore_mode(mode),
        verification=parse_restore_verification(verify),
    )
    plan = build_restore_plan(intent)

    run_id = str(plan.run_id)

    stage_root = (
        destination_root.with_name(f"{destination_root.name}.wcbt_stage") / run_id / "stage_root"
    )

    if dry_run:
        artifacts_root = _restore_artifacts_root(plan.destination_root, run_id)
    else:
        # Artifacts must live inside the staged tree so they survive atomic promotion.
        artifacts_root = stage_root / ".wcbt_restore" / run_id

    artifacts_root.mkdir(parents=True, exist_ok=True)
    stage_root.mkdir(parents=True, exist_ok=True)

    journal = RestoreExecutionJournal(
        artifacts_root / "execution_journal.jsonl",
        clock=clock_to_use,
    )

    journal.append(
        "restore_run_started",
        {
            "manifest_path": str(manifest_path),
            "destination_root": str(destination_root),
            "mode": str(mode),
            "verify": str(verify),
            "schema_version": str(schema_version),
            "operations_count": len(operations),
            "dry_run": dry_run,
        },
    )

    plan_dict = plan.to_dict()
    source_manifest = plan_dict.get("source_manifest")
    if not isinstance(source_manifest, Mapping):
        raise RestoreArtifactError("Restore plan serialization missing 'source_manifest' mapping.")

    plan_dict["source_manifest"]["operations_count"] = len(operations)
    plan_dict["source_manifest"]["schema_version"] = str(run_payload.get("schema_version", ""))
    plan_dict["source_manifest"]["run_id"] = str(run_payload.get("run_id", ""))
    plan_dict["source_manifest"]["archive_root"] = str(run_payload.get("archive_root", ""))
    plan_dict["source_manifest"]["profile_name"] = str(run_payload.get("profile_name", ""))

    journal.append("restore_plan_built", {"run_id": str(plan.run_id)})

    plan_for_materialize = plan
    object.__setattr__(
        plan_for_materialize,
        "source_manifest",
        {**plan.source_manifest, "_operations_full": operations},
    )

    candidates = materialize_restore_candidates(plan_for_materialize)
    journal.append("restore_candidates_materialized", {"candidates_count": len(candidates)})

    restore_plan_path = artifacts_root / "restore_plan.json"
    restore_candidates_path = artifacts_root / "restore_candidates.jsonl"

    _write_json(restore_plan_path, plan_dict)
    _write_jsonl(restore_candidates_path, [c.to_dict() for c in candidates])

    base_counts: dict[str, int] = {
        "planned": len(candidates),
        "conflicts": 0,
        "staged_files": 0,
        "failed_files": 0,
        "verified_files": 0,
    }

    # Enforce add-only conflict policy (artifact-first, fail-fast)
    if intent.mode.value == "add-only":
        conflicting = [c for c in candidates if c.operation_type.value == "skip_existing"]

        if conflicting:
            conflicts_path = artifacts_root / "restore_conflicts.jsonl"

            _write_jsonl(
                conflicts_path,
                [
                    {
                        "operation_index": c.operation_index,
                        "relative_path": c.relative_path,
                        "destination_path": str(c.destination_path),
                        "reason": c.reason,
                    }
                    for c in conflicting
                ],
            )

            journal.append(
                "restore_conflicts_detected",
                {
                    "conflicts_count": len(conflicting),
                    "conflicts_path": str(conflicts_path),
                },
            )

            _write_restore_summary(
                artifacts_root=artifacts_root,
                run_id=run_id,
                manifest_path=manifest_path,
                destination_root=destination_root,
                mode=mode,
                verify=verify,
                dry_run=dry_run,
                result="conflict",
                counts={**base_counts, "conflicts": len(conflicting)},
            )

            raise RestoreConflictError(
                f"Restore conflicts detected in add-only mode: {len(conflicting)} file(s) already exist."
            )

    journal.append(
        "restore_artifacts_written",
        {
            "restore_plan_path": str(restore_plan_path),
            "restore_candidates_path": str(restore_candidates_path),
        },
    )

    stage_root = (
        destination_root.with_name(f"{destination_root.name}.wcbt_stage")
        / str(plan.run_id)
        / "stage_root"
    )

    try:
        if not dry_run:
            _run_pre_restore_backup(
                run_payload=run_payload,
                destination_root=destination_root,
                data_root=data_root,
                clock=clock_to_use,
                journal=journal,
            )

        stage_result = build_restore_stage(
            candidates=candidates,
            stage_root=stage_root,
            dry_run=dry_run,
            journal=journal,
            artifacts_root=artifacts_root,
        )

        verification_result = verify_restore_stage(
            candidates=candidates,
            stage_root=stage_root,
            verification_mode=verify,
            dry_run=dry_run,
            journal=journal,
            artifacts_root=artifacts_root,
        )

        journal.append(
            "restore_stage_verified",
            {
                "verification_mode": verification_result.verification_mode,
                "planned_files": verification_result.planned_files,
                "verified_files": verification_result.verified_files,
                "staged_files": stage_result.staged_files,
            },
        )

        if dry_run:
            journal.append(
                "restore_promotion_skipped",
                {
                    "reason": "dry_run",
                    "destination_root": str(destination_root),
                    "stage_root": str(stage_root),
                },
            )

            _write_restore_summary(
                artifacts_root=artifacts_root,
                run_id=run_id,
                manifest_path=manifest_path,
                destination_root=destination_root,
                mode=mode,
                verify=verify,
                dry_run=True,
                result="ok",
                counts={
                    **base_counts,
                    "staged_files": int(stage_result.staged_files),
                    "failed_files": int(getattr(stage_result, "failed_files", 0)),
                    "verified_files": int(verification_result.verified_files),
                },
            )
            return RestoreRunResult(
                run_id=run_id,
                manifest_path=manifest_path,
                destination_root=destination_root,
                dry_run=True,
                mode=mode,
                verify=verify,
                artifacts_root=artifacts_root,
                stage_root=stage_root,
                summary_path=artifacts_root / "restore_summary.json",
            )

        _write_restore_summary(
            artifacts_root=artifacts_root,
            run_id=run_id,
            manifest_path=manifest_path,
            destination_root=destination_root,
            mode=mode,
            verify=verify,
            dry_run=False,
            result="ok",
            counts={
                **base_counts,
                "staged_files": int(stage_result.staged_files),
                "failed_files": int(getattr(stage_result, "failed_files", 0)),
                "verified_files": int(verification_result.verified_files),
            },
        )

        journal.append(
            "restore_promotion_started",
            {
                "destination_root": str(destination_root),
                "stage_root": str(stage_root),
                "run_id": str(plan.run_id),
            },
        )

        promote_stage_to_destination(
            stage_root=stage_root,
            destination_root=destination_root,
            run_id=str(plan.run_id),
            dry_run=False,
            journal=None,
        )

        final_artifacts_root = _restore_artifacts_root(destination_root, run_id)
        return RestoreRunResult(
            run_id=run_id,
            manifest_path=manifest_path,
            destination_root=destination_root,
            dry_run=False,
            mode=mode,
            verify=verify,
            artifacts_root=final_artifacts_root,
            stage_root=stage_root,
            summary_path=final_artifacts_root / "restore_summary.json",
        )

    except Exception:  # noqa: BLE001
        # Best-effort: record an error summary, then re-raise.
        try:
            _write_restore_summary(
                artifacts_root=artifacts_root,
                run_id=run_id,
                manifest_path=manifest_path,
                destination_root=destination_root,
                mode=mode,
                verify=verify,
                dry_run=dry_run,
                result="error",
                counts=base_counts,
            )
        except Exception:  # noqa: BLE001
            pass
        raise
