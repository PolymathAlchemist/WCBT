"""
Manifest I/O and discovery.

Manifests are canonical, and this module provides atomic read/write helpers plus
directory discovery. It intentionally does not depend on SQLite.

Design constraints
------------------
- Writes are atomic (temp file + replace) to preserve invariants.
- No secrets are ever written into manifests by design; this module simply persists models.
- All serialization is deterministic for a given in-memory object.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

from .data_models import BackupManifest
from .exceptions import ManifestIOError, ManifestValidationError


@dataclass(frozen=True, slots=True)
class ManifestWriteOptions:
    """Options controlling manifest serialization."""

    pretty: bool = True
    indent: int = 2
    sort_keys: bool = False
    ensure_ascii: bool = False


@dataclass(frozen=True, slots=True)
class BackupRunManifestV1:
    """
    Minimal canonical manifest for a single backup run.

    This manifest is intended for the milestone where the run is materialized
    (directory created, plan and manifest written) without copying or deleting
    any files yet.

    Attributes
    ----------
    schema_version:
        Schema identifier for forward compatibility.
    run_id:
        Stable ID for the run directory and manifest association.
    created_at_utc:
        ISO-8601 timestamp in UTC, produced by an injectable clock.
    archive_root:
        Absolute archive run directory for this run.
    plan_text_path:
        Absolute path to the plan text file for this run.
    operations:
        Planned operations in deterministic order, serialized into a JSON-safe structure.
    scan_issues:
        Non-fatal scan issues captured during scanning, serialized into a JSON-safe structure.
    """

    schema_version: str
    run_id: str
    created_at_utc: str
    archive_root: str
    plan_text_path: str
    operations: list[Mapping[str, Any]]
    scan_issues: list[Mapping[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the manifest to a JSON-serializable dictionary.

        Returns
        -------
        dict[str, Any]
            JSON-ready payload.
        """
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "created_at_utc": self.created_at_utc,
            "archive_root": self.archive_root,
            "plan_text_path": self.plan_text_path,
            "operations": list(self.operations),
            "scan_issues": list(self.scan_issues),
        }


def read_manifest(manifest_path: Path) -> BackupManifest:
    """
    Read and validate a manifest from disk.

    Parameters
    ----------
    manifest_path
        Path to a JSON manifest file.

    Returns
    -------
    BackupManifest
        Parsed and validated manifest.

    Raises
    ------
    ManifestIOError
        If the file cannot be read or decoded.
    ManifestValidationError
        If the file parses but fails schema validation.
    """
    try:
        raw_text = manifest_path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except OSError as exc:
        raise ManifestIOError(f"Failed to read manifest: {manifest_path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestIOError(f"Invalid JSON in manifest: {manifest_path}") from exc

    try:
        return BackupManifest.from_dict(payload)
    except ValueError as exc:
        raise ManifestValidationError(f"Manifest validation failed: {manifest_path}") from exc


def write_json_atomic(
    json_path: Path,
    payload: Mapping[str, Any],
    *,
    options: ManifestWriteOptions | None = None,
) -> None:
    """
    Atomically write a JSON payload to disk.

    The file is written to a temporary path within the same directory, then
    replaced using os.replace to guarantee atomicity on Windows NTFS.

    Parameters
    ----------
    json_path
        Target JSON path.
    payload
        JSON-serializable mapping.
    options
        Serialization options.

    Raises
    ------
    ManifestIOError
        If the JSON cannot be written.
    """
    options = options or ManifestWriteOptions()

    try:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = json_path.with_suffix(json_path.suffix + ".tmp")
        json_text = json.dumps(
            dict(payload),
            indent=options.indent if options.pretty else None,
            separators=None if options.pretty else (",", ":"),
            sort_keys=options.sort_keys,
            ensure_ascii=options.ensure_ascii,
        )
        tmp_path.write_text(json_text, encoding="utf-8", newline="\n")
        os.replace(tmp_path, json_path)
    except OSError as exc:
        raise ManifestIOError(f"Failed to write JSON: {json_path}") from exc


def write_manifest_atomic(
    manifest_path: Path,
    manifest: BackupManifest,
    *,
    options: ManifestWriteOptions | None = None,
) -> None:
    """
    Atomically write a manifest to disk.

    Parameters
    ----------
    manifest_path
        Target manifest path.
    manifest
        Manifest object to serialize.
    options
        Serialization options.

    Raises
    ------
    ManifestIOError
        If the manifest cannot be written.
    """
    write_json_atomic(manifest_path, manifest.to_dict(), options=options)


def write_run_manifest_atomic(
    manifest_path: Path,
    manifest: BackupRunManifestV1,
    *,
    options: ManifestWriteOptions | None = None,
) -> None:
    """
    Atomically write a BackupRunManifestV1 to disk.

    Parameters
    ----------
    manifest_path
        Target manifest path.
    manifest
        Backup run manifest object to serialize.
    options
        Serialization options.

    Raises
    ------
    ManifestIOError
        If the manifest cannot be written.
    """
    write_json_atomic(manifest_path, manifest.to_dict(), options=options)


def iter_manifest_paths(manifest_root: Path) -> Iterator[Path]:
    """
    Yield manifest file paths under a directory tree.

    Parameters
    ----------
    manifest_root
        Root directory containing manifest files.

    Yields
    ------
    pathlib.Path
        Paths ending in .json.

    Notes
    -----
    This is intentionally dumb and deterministic: it yields in sorted path order.
    """
    if not manifest_root.exists():
        return iter(())
    paths = sorted(p for p in manifest_root.rglob("*.json") if p.is_file())
    return iter(paths)


def load_all_manifests(manifest_root: Path) -> list[BackupManifest]:
    """
    Load all manifests under a root directory.

    Parameters
    ----------
    manifest_root
        Root directory to scan.

    Returns
    -------
    list[BackupManifest]
        Valid manifests.
    """
    manifests: list[BackupManifest] = []
    for path in iter_manifest_paths(manifest_root):
        manifests.append(read_manifest(path))
    return manifests
