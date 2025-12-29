"""
Materialize a backup run without copying or deleting files.

This module creates a concrete run directory, persists the human-readable plan,
and writes a canonical run manifest, while still leaving the backup as "dry-run"
in the sense that no file operations against the archive contents occur yet.

Design constraints
------------------
- Safe: no copying, no deletion, no modification of source files.
- Deterministic: same plan and run metadata yield identical plan and manifest content.
- Atomic writes: manifest and plan use temp + replace.
- Invariants enforced:
  - Run directory must not already exist.
  - plan.txt is written before manifest.json.
  - manifest.json only exists if plan.txt exists.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from backup_engine.backup.plan import BackupPlan, serialize_plan_for_manifest
from backup_engine.errors import BackupMaterializationError
from backup_engine.manifest_store import BackupRunManifestV1, write_run_manifest_atomic


class Clock(Protocol):
    """
    Injectable clock interface.

    Implementations should provide UTC timestamps deterministically for tests.

    Methods
    -------
    now_utc_isoformat() -> str
        Return an ISO-8601 string in UTC.
    """

    def now_utc_isoformat(self) -> str:  # pragma: no cover
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class MaterializePaths:
    """
    Concrete file paths produced by materialization.

    Attributes
    ----------
    run_root:
        The run directory created for this run.
    plan_text_path:
        Path to plan.txt within the run directory.
    manifest_path:
        Path to manifest.json within the run directory.
    """

    run_root: Path
    plan_text_path: Path
    manifest_path: Path


def materialize_backup_run(
    *,
    plan: BackupPlan,
    run_root: Path,
    run_id: str,
    plan_text: str,
    clock: Clock,
) -> MaterializePaths:
    """
    Materialize a backup run directory and persist plan and manifest.

    Parameters
    ----------
    plan:
        Backup plan to persist.
    run_root:
        Run directory to create. Must not exist.
    run_id:
        Stable identifier for the run, used in the manifest.
    plan_text:
        Human-readable plan output that will be written to plan.txt.
        This should already be deterministic (rendered elsewhere).
    clock:
        Injectable clock for created_at_utc.

    Returns
    -------
    MaterializePaths
        Paths created for the run.

    Raises
    ------
    BackupMaterializationError
        If the run cannot be created or invariants fail.

    Invariants
    ----------
    - run_root is created exactly once and must be new.
    - plan.txt is written before manifest.json.
    - manifest.json is written atomically and only after plan.txt exists.
    """
    resolved_run_root = run_root.resolve()
    _create_new_directory(resolved_run_root)

    plan_text_path = resolved_run_root / "plan.txt"
    manifest_path = resolved_run_root / "manifest.json"

    _write_text_atomic(plan_text_path, plan_text)

    if not plan_text_path.is_file():
        raise BackupMaterializationError(f"Plan file was not created as expected: {plan_text_path}")

    operations_payload, scan_issues_payload = serialize_plan_for_manifest(plan)

    manifest = BackupRunManifestV1(
        schema_version="wcbt.backup_run_manifest.v1",
        run_id=run_id,
        created_at_utc=clock.now_utc_isoformat(),
        archive_root=str(plan.archive_root),
        plan_text_path=str(plan_text_path),
        operations=operations_payload,
        scan_issues=scan_issues_payload,
    )

    write_run_manifest_atomic(manifest_path, manifest)

    if not manifest_path.is_file():
        raise BackupMaterializationError(f"Manifest file was not created as expected: {manifest_path}")

    return MaterializePaths(run_root=resolved_run_root, plan_text_path=plan_text_path, manifest_path=manifest_path)


def _create_new_directory(directory: Path) -> None:
    """
    Create a new directory, failing if it already exists.

    Parameters
    ----------
    directory:
        Directory to create.

    Raises
    ------
    BackupMaterializationError
        If the directory exists or cannot be created.
    """
    if directory.exists():
        raise BackupMaterializationError(f"Run directory already exists: {directory}")

    try:
        directory.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        raise BackupMaterializationError(f"Failed to create run directory: {directory}") from exc


def _write_text_atomic(path: Path, text: str) -> None:
    """
    Atomically write text to disk.

    Parameters
    ----------
    path:
        Target path.
    text:
        Text to write.

    Raises
    ------
    BackupMaterializationError
        If the file cannot be written.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(text, encoding="utf-8", newline="\n")
        os.replace(tmp_path, path)
    except OSError as exc:
        raise BackupMaterializationError(f"Failed to write text file: {path}") from exc
