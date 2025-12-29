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


def read_manifest(manifest_path: Path) -> BackupManifest:
    """
    Read and validate a BackupManifest from disk.
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
    """Atomically write a validated BackupManifest to disk."""
    manifest.validate()
    write_json_atomic(manifest_path, manifest.to_dict(), options=options)


def write_run_manifest_atomic(
    manifest_path: Path,
    manifest: SupportsToDict,
    *,
    options: ManifestWriteOptions | None = None,
) -> None:
    """Atomically write a run manifest to disk."""
    write_json_atomic(manifest_path, manifest.to_dict(), options=options)


def iter_manifest_paths(manifest_root: Path) -> Iterator[Path]:
    """Yield manifest file paths under a directory tree."""
    for path in manifest_root.rglob("*.json"):
        if path.is_file():
            yield path


def load_all_manifests(manifest_root: Path) -> list[BackupManifest]:
    """Load all backup manifests under a manifest root."""
    manifests: list[BackupManifest] = []
    for path in iter_manifest_paths(manifest_root):
        manifests.append(read_manifest(path))
    return manifests
