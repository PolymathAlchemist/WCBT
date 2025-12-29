"""
Materialize a backup run directory and write canonical artifacts.

This module performs the first safe "side effect" phase of a backup run:
- create the run directory
- write plan.txt
- write run manifest.json (atomic)

No file copying or deletion occurs in this phase.

Design notes
------------
- This module is deterministic given its inputs and an injectable Clock.
- It uses the manifest store for atomic writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from backup_engine.backup.plan import BackupPlan, serialize_plan_for_manifest
from backup_engine.clock import Clock
from backup_engine.errors import BackupMaterializationError
from backup_engine.manifest_store import BackupRunManifestV2, write_run_manifest_atomic


@dataclass(frozen=True, slots=True)
class MaterializedBackupRun:
    """
    Result of materializing a backup run directory.

    Attributes
    ----------
    run_root:
        The created run directory.
    plan_text_path:
        Path to the written plan.txt.
    manifest_path:
        Path to the written manifest.json.
    manifest:
        The run manifest object that was written.
    """

    run_root: Path
    plan_text_path: Path
    manifest_path: Path
    manifest: BackupRunManifestV2


def materialize_backup_run(
    *,
    plan: BackupPlan,
    run_root: Path,
    run_id: str,
    plan_text: str,
    profile_name: str,
    source_root: Path,
    clock: Clock,
) -> MaterializedBackupRun:
    """
    Create the run directory and write plan + manifest artifacts.
    """
    try:
        run_root.mkdir(parents=True, exist_ok=False)

        plan_text_path = run_root / "plan.txt"
        _write_text_file(path=plan_text_path, content=plan_text)

        created_at_utc = _ensure_utc_datetime(clock.now())
        operations_payload, scan_issues_payload = serialize_plan_for_manifest(plan)

        manifest = BackupRunManifestV2(
            schema_version=BackupRunManifestV2.SCHEMA_VERSION,
            run_id=run_id,
            created_at_utc=created_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            archive_root=str(plan.archive_root),
            plan_text_path=str(plan_text_path),
            profile_name=profile_name,
            source_root=str(source_root),
            operations=list(operations_payload),
            scan_issues=list(scan_issues_payload),
            execution=None,
        )

        manifest_path = run_root / "manifest.json"
        write_run_manifest_atomic(manifest_path, manifest)

        return MaterializedBackupRun(
            run_root=run_root,
            plan_text_path=plan_text_path,
            manifest_path=manifest_path,
            manifest=manifest,
        )
    except FileExistsError as exc:
        raise BackupMaterializationError(f"Run directory already exists: {run_root}") from exc
    except OSError as exc:
        raise BackupMaterializationError(f"Failed to materialize run directory: {run_root} ({exc!s})") from exc


def _write_text_file(*, path: Path, content: str) -> None:
    """
    Write a UTF-8 text file with deterministic newlines.
    """
    path.write_text(content, encoding="utf-8", newline="\n")


def _ensure_utc_datetime(value: datetime) -> datetime:
    """Ensure a datetime is timezone-aware and normalized to UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
