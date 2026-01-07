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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Iterator, Mapping, Protocol

from .data_models import BackupManifest
from .exceptions import ManifestIOError, ManifestValidationError


@dataclass(frozen=True, slots=True)
class ManifestWriteOptions:
    """Options controlling manifest serialization."""

    pretty: bool = True
    indent: int = 2
    sort_keys: bool = True
    ensure_ascii: bool = False


class SupportsToDict(Protocol):
    """Protocol for manifest models that can be serialized to JSON."""

    def to_dict(self) -> dict[str, Any]:
        """Convert the manifest to a JSON-serializable dictionary."""
        ...


@dataclass(frozen=True, slots=True)
class RunOperationResultV1:
    """
    Per-operation execution result (v1).

    Attributes
    ----------
    operation_index:
        Index into the original deterministic plan operations list.
    operation_type:
        Planned operation type string.
    relative_path:
        Relative file path string.
    source_path:
        Absolute source path string.
    destination_path:
        Absolute destination path string.
    outcome:
        Outcome string (stable external contract).
    message:
        Human-readable detail for the outcome.
    """

    operation_index: int
    operation_type: str
    relative_path: str
    source_path: str
    destination_path: str
    outcome: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable payload."""
        return {
            "operation_index": self.operation_index,
            "operation_type": self.operation_type,
            "relative_path": self.relative_path,
            "source_path": self.source_path,
            "destination_path": self.destination_path,
            "outcome": self.outcome,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class BackupRunExecutionV1:
    """
    Execution results container for a backup run.

    Attributes
    ----------
    status:
        Overall status string: "success" or "failed".
    results:
        Per-operation results in deterministic order (same order as the plan).
    """

    status: str
    results: list[RunOperationResultV1]

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable payload."""
        return {
            "status": self.status,
            "results": [r.to_dict() for r in self.results],
        }


@dataclass(frozen=True, slots=True)
class BackupRunManifestV2:
    """
    Canonical manifest for a single backup run (schema v2).

    This manifest is written during materialization (execution=None) and may be
    updated after execution to include deterministic outcomes.
    """

    SCHEMA_VERSION: ClassVar[str] = "wcbt_run_manifest_v2"

    schema_version: str
    run_id: str
    created_at_utc: str
    archive_root: str
    plan_text_path: str
    profile_name: str
    source_root: str
    operations: list[Mapping[str, Any]] = field(default_factory=list)
    scan_issues: list[Mapping[str, Any]] = field(default_factory=list)
    execution: BackupRunExecutionV1 | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the manifest to a JSON-serializable dictionary."""
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "created_at_utc": self.created_at_utc,
            "archive_root": self.archive_root,
            "plan_text_path": self.plan_text_path,
            "profile_name": self.profile_name,
            "source_root": self.source_root,
            "operations": list(self.operations),
            "scan_issues": list(self.scan_issues),
        }
        if self.execution is not None:
            payload["execution"] = self.execution.to_dict()
        return payload


def read_manifest_json(manifest_path: Path) -> dict[str, Any]:
    """
    Read a manifest JSON file into a mutable dictionary.

    Parameters
    ----------
    manifest_path:
        Path to manifest.json.

    Returns
    -------
    dict[str, Any]
        Parsed manifest payload.

    Raises
    ------
    ManifestIOError
        If the file cannot be read or parsed.
    """
    try:
        text = manifest_path.read_text(encoding="utf-8")
        payload = json.loads(text)
    except OSError as exc:
        raise ManifestIOError(f"Failed to read manifest: {manifest_path}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestIOError(f"Invalid JSON in manifest: {manifest_path}") from exc

    if not isinstance(payload, dict):
        raise ManifestIOError(f"Manifest must be a JSON object: {manifest_path}")
    return payload


def write_manifest_json_atomic(manifest_path: Path, payload: Mapping[str, Any]) -> None:
    """
    Atomically write a manifest JSON payload.

    Parameters
    ----------
    manifest_path:
        Path to manifest.json.
    payload:
        JSON-serializable mapping.

    Notes
    -----
    This preserves the atomic write invariant (temp + replace).
    """
    write_json_atomic(manifest_path, payload)


def read_manifest(manifest_path: Path) -> BackupManifest:
    """
    Read and validate a BackupManifest from disk.

    Parameters
    ----------
    manifest_path:
        Path to a manifest JSON file.

    Returns
    -------
    BackupManifest
        Validated manifest model.

    Raises
    ------
    ManifestIOError
        If the file cannot be read or parsed.
    ManifestValidationError
        If the parsed payload fails schema validation.
    """
    try:
        text = manifest_path.read_text(encoding="utf-8")
        payload = json.loads(text)
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
    Write JSON atomically to disk.

    Parameters
    ----------
    json_path:
        Target JSON file path.
    payload:
        JSON-serializable mapping to persist.
    options:
        Serialization options (indentation, key ordering, ASCII handling).

    Notes
    -----
    - Uses a temp file followed by an atomic replace to preserve invariants.
    - Creates parent directories as needed.
    - Writes LF newlines for deterministic artifacts across platforms.

    Raises
    ------
    ManifestIOError
        If the payload cannot be written.
    """
    opts = options or ManifestWriteOptions()
    json_path = json_path.expanduser()
    directory = json_path.parent
    directory.mkdir(parents=True, exist_ok=True)

    temp_path = json_path.with_suffix(json_path.suffix + ".tmp")

    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            if opts.pretty:
                json.dump(
                    payload,
                    handle,
                    indent=opts.indent,
                    sort_keys=opts.sort_keys,
                    ensure_ascii=opts.ensure_ascii,
                )
                handle.write("\n")
            else:
                json.dump(
                    payload,
                    handle,
                    separators=(",", ":"),
                    sort_keys=opts.sort_keys,
                    ensure_ascii=opts.ensure_ascii,
                )
        os.replace(temp_path, json_path)
    except OSError as exc:
        try:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise ManifestIOError(f"Failed to write JSON: {json_path} ({exc!s})") from exc


def write_manifest_atomic(
    manifest_path: Path,
    manifest: BackupManifest,
    *,
    options: ManifestWriteOptions | None = None,
) -> None:
    """
    Atomically write a validated BackupManifest to disk.

    Parameters
    ----------
    manifest_path:
        Target manifest file path.
    manifest:
        Manifest model to validate and persist.
    options:
        Serialization options.

    Raises
    ------
    ManifestValidationError
        If the manifest fails validation.
    ManifestIOError
        If the manifest cannot be written.
    """
    manifest.validate()
    write_json_atomic(manifest_path, manifest.to_dict(), options=options)


def write_run_manifest_atomic(
    manifest_path: Path,
    manifest: SupportsToDict,
    *,
    options: ManifestWriteOptions | None = None,
) -> None:
    """
    Atomically write a run manifest to disk.

    Parameters
    ----------
    manifest_path:
        Target manifest file path.
    manifest:
        Run manifest model that can serialize to a JSON dictionary.
    options:
        Serialization options.

    Raises
    ------
    ManifestIOError
        If the manifest cannot be written.
    """
    write_json_atomic(manifest_path, manifest.to_dict(), options=options)


def iter_manifest_paths(manifest_root: Path) -> Iterator[Path]:
    """
    Yield JSON file paths under a directory tree.

    Parameters
    ----------
    manifest_root:
        Root directory to scan.

    Yields
    ------
    pathlib.Path
        Paths to files matching `*.json`.
    """
    for path in manifest_root.rglob("*.json"):
        if path.is_file():
            yield path


def load_all_manifests(manifest_root: Path) -> list[BackupManifest]:
    """
    Load and validate all BackupManifest files under a root directory.

    Parameters
    ----------
    manifest_root:
        Root directory to scan.

    Returns
    -------
    list[BackupManifest]
        Validated manifests.

    Raises
    ------
    ManifestIOError
        If a manifest cannot be read or parsed.
    ManifestValidationError
        If a manifest fails validation.
    """
    manifests: list[BackupManifest] = []
    for path in iter_manifest_paths(manifest_root):
        manifests.append(read_manifest(path))
    return manifests


@dataclass(frozen=True, slots=True)
class BackupRunSummary:
    """
    Minimal, stable summary of a backup run discovered via manifest scanning.

    This is intentionally lightweight and tolerant of partial/corrupt manifests.
    It is suitable for UI listing, not for restore planning.
    """

    run_id: str
    manifest_path: Path
    modified_at_utc: str
    created_at_utc: str | None
    archive_root: str | None
    profile_name: str | None
    source_root: str | None


def list_backup_runs(
    archive_root: Path,
    *,
    profile_name: str | None = None,
    limit: int = 500,
) -> list[BackupRunSummary]:
    """
    Discover backup runs by scanning an archive root for run manifests.

    Parameters
    ----------
    archive_root:
        Root directory to scan for `manifest.json` files.
    limit:
        Maximum number of discovered runs to return (after sorting by mtime desc).

    Returns
    -------
    list[BackupRunSummary]
        Discovered runs sorted by manifest mtime descending.

    Notes
    -----
    - This function is intentionally tolerant: unreadable or invalid JSON files
      are skipped.
    - Only schema_version 'wcbt_run_manifest_v2' is returned.
    - Sorting uses filesystem mtime to reflect "most recently written" runs.
    """
    root = archive_root.expanduser()
    if not root.exists() or not root.is_dir():
        return []

    candidates: list[tuple[float, Path]] = []
    try:
        for mp in root.rglob("manifest.json"):
            if mp.is_file():
                try:
                    candidates.append((mp.stat().st_mtime, mp))
                except OSError:
                    candidates.append((0.0, mp))
    except OSError:
        return []

    candidates.sort(key=lambda t: t[0], reverse=True)

    out: list[BackupRunSummary] = []
    for mtime, mp in candidates:
        if len(out) >= limit:
            break

        try:
            payload = read_manifest_json(mp)
        except ManifestIOError:
            continue

        if payload.get("schema_version") != "wcbt_run_manifest_v2":
            continue

        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            continue

        if profile_name is not None:
            payload_profile = payload.get("profile_name")
            if payload_profile != profile_name:
                continue

        modified_at_utc = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

        def _opt_str(key: str) -> str | None:
            v = payload.get(key)
            return v if isinstance(v, str) and v else None

        out.append(
            BackupRunSummary(
                run_id=run_id,
                manifest_path=mp,
                modified_at_utc=modified_at_utc,
                created_at_utc=_opt_str("created_at_utc"),
                archive_root=_opt_str("archive_root"),
                profile_name=_opt_str("profile_name"),
                source_root=_opt_str("source_root"),
            )
        )

    return out
