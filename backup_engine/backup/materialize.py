"""
Materialize a backup run directory and write canonical artifacts.

This module performs the first safe "side effect" phase of a backup run:
- create the run directory
- write plan.txt
- write manifest.json (atomic)

No file copying or deletion occurs in this phase.

Design notes
------------
- This module is deterministic given its inputs and an injectable Clock.
- It uses the canonical manifest store for atomic writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from backup_engine.backup.plan import BackupPlan
from backup_engine.clock import Clock
from backup_engine.data_models import (
    ArchiveFormat,
    ArchiveInfo,
    BackupManifest,
    EnvironmentInfo,
    ManifestSchemaVersion,
    SourceType,
    DedicatedServerSource,
)
from backup_engine.errors import BackupMaterializationError
from backup_engine.manifest_store import write_manifest_atomic


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
        The manifest object that was persisted.
    """

    run_root: Path
    plan_text_path: Path
    manifest_path: Path
    manifest: BackupManifest


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

    Parameters
    ----------
    plan:
        Planned operations for this run.
    run_root:
        The run directory to create (must be unique per run).
    run_id:
        A stable, printable identifier for the run directory name.
    plan_text:
        Rendered plan report text to persist as plan.txt.
    profile_name:
        Profile name associated with this run.
    source_root:
        The validated source root for this run.
    clock:
        Injectable clock for deterministic timestamps.

    Returns
    -------
    MaterializedBackupRun
        Details about created paths and the persisted manifest.

    Raises
    ------
    BackupMaterializationError
        If the run directory or artifacts cannot be created safely.
    """
    try:
        run_root = run_root.expanduser()
        run_root.mkdir(parents=True, exist_ok=False)

        plan_text_path = run_root / "plan.txt"
        _write_text_file(path=plan_text_path, content=plan_text)

        created_at_utc = _ensure_utc_datetime(clock.now())

        backup_id = _derive_backup_id(profile_name=profile_name, run_id=run_id)
        world_id = _derive_world_id(profile_name=profile_name, source_root=source_root)

        manifest = _build_manifest(
            profile_name=profile_name,
            backup_id=backup_id,
            world_id=world_id,
            created_at_utc=created_at_utc,
            source_root=source_root,
            archive_root=plan.archive_root,
        )

        manifest_path = run_root / "manifest.json"
        write_manifest_atomic(manifest_path, manifest)

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
    except ValueError as exc:
        raise BackupMaterializationError(f"Failed to build manifest: {exc!s}") from exc


def _write_text_file(*, path: Path, content: str) -> None:
    """
    Write a UTF-8 text file with deterministic newlines.

    Parameters
    ----------
    path:
        Output file path.
    content:
        Text content to write.

    Raises
    ------
    OSError
        If writing fails.
    """
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def _ensure_utc_datetime(value: datetime) -> datetime:
    """
    Convert a datetime to a timezone-aware UTC datetime.

    Parameters
    ----------
    value:
        Datetime from the injected clock.

    Returns
    -------
    datetime
        A timezone-aware UTC datetime.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _derive_backup_id(*, profile_name: str, run_id: str) -> UUID:
    """
    Derive a deterministic backup_id for this run.

    Parameters
    ----------
    profile_name:
        Profile name.
    run_id:
        Run identifier string.

    Returns
    -------
    UUID
        Deterministic UUID derived from profile and run identifier.
    """
    return uuid5(NAMESPACE_URL, f"wcbt:backup:{profile_name}:{run_id}")


def _derive_world_id(*, profile_name: str, source_root: Path) -> UUID:
    """
    Derive a deterministic world_id for a given source root.

    Parameters
    ----------
    profile_name:
        Profile name.
    source_root:
        Source root path.

    Returns
    -------
    UUID
        Deterministic UUID derived from profile and normalized source root.
    """
    normalized = str(source_root.resolve())
    return uuid5(NAMESPACE_URL, f"wcbt:world:{profile_name}:{normalized}")


def _build_manifest(
    *,
    profile_name: str,
    backup_id: UUID,
    world_id: UUID,
    created_at_utc: datetime,
    source_root: Path,
    archive_root: Path,
) -> BackupManifest:
    """
    Build a minimal valid manifest for a materialized (pre-copy) run.

    Parameters
    ----------
    profile_name:
        Profile name.
    backup_id:
        Deterministic run ID.
    world_id:
        Deterministic source/world ID.
    created_at_utc:
        UTC timestamp for the run.
    source_root:
        Source root path.
    archive_root:
        Archive root path for this run.

    Returns
    -------
    BackupManifest
        Valid manifest instance.
    """
    environment = EnvironmentInfo(
        minecraft_version="unknown",
        loader="unknown",
        loader_version="unknown",
        java_version="unknown",
    )

    source = DedicatedServerSource(
        type=SourceType.DEDICATED_SERVER,
        server_root=str(source_root),
        world_folder="unknown",
    )

    archive = ArchiveInfo(
        format=ArchiveFormat.ZIP,
        filename=str(archive_root),
        size_bytes=0,
        sha256=None,
    )

    manifest = BackupManifest.new(
        backup_id=backup_id,
        world_id=world_id,
        created_at_utc=created_at_utc,
        profile_name=profile_name,
        environment=environment,
        source=source,
        archive=archive,
    )
    manifest.validate()
    return manifest
