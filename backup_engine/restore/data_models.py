from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


class RestoreMode(Enum):
    """How restore should treat existing destination files."""

    ADD_ONLY = "add-only"
    OVERWRITE = "overwrite"


class RestoreVerification(Enum):
    """Verification level recorded in the plan (execute/verify comes later)."""

    NONE = "none"
    SIZE = "size"


class RestoreOperationType(Enum):
    """Planned operation type for a restore candidate."""

    COPY_NEW = "copy_new"
    OVERWRITE_EXISTING = "overwrite_existing"
    SKIP_EXISTING = "skip_existing"


@dataclass(frozen=True, slots=True)
class RestoreIntent:
    """
    Restore intent.

    Parameters
    ----------
    manifest_path:
        Path to the backup run manifest.json (schema wcbt_run_manifest_v2).
    destination_root:
        User-visible root directory where files would be restored.
    mode:
        Restore mode (add-only or overwrite).
    verification:
        Verification level to record in the restore plan.
    """

    manifest_path: Path
    destination_root: Path
    mode: RestoreMode
    verification: RestoreVerification


@dataclass(frozen=True, slots=True)
class RestorePlan:
    """
    Planned restore run, derived from a run manifest.

    Attributes
    ----------
    schema_version:
        Schema tag for this plan artifact.
    execution_strategy:
        Reserved for later milestones; recorded now to lock intent.
    run_id:
        Run ID from the source manifest.
    created_at_utc:
        Timestamp copied from source manifest.
    manifest_path:
        Path to the source manifest file.
    archive_root:
        Archive root from which restore reads bytes.
    destination_root:
        Restore target root (user-visible).
    profile_name:
        Profile name from source manifest.
    mode:
        Restore mode.
    verification:
        Recorded verification intent.
    source_manifest:
        Minimal copy of the source manifest fields needed for deterministic planning.
    """

    schema_version: str
    execution_strategy: str
    run_id: str
    created_at_utc: str
    manifest_path: Path
    archive_root: Path
    destination_root: Path
    profile_name: str
    mode: RestoreMode
    verification: RestoreVerification
    source_manifest: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable payload."""
        return {
            "schema_version": self.schema_version,
            "execution_strategy": self.execution_strategy,
            "run_id": self.run_id,
            "created_at_utc": self.created_at_utc,
            "manifest_path": str(self.manifest_path),
            "archive_root": str(self.archive_root),
            "destination_root": str(self.destination_root),
            "profile_name": self.profile_name,
            "mode": self.mode.value,
            "verification": self.verification.value,
            "source_manifest": dict(self.source_manifest),
        }


@dataclass(frozen=True, slots=True)
class RestoreCandidate:
    """
    A single restore candidate operation.

    Attributes
    ----------
    operation_index:
        Index into the source manifest operations list.
    relative_path:
        Relative path string from the manifest (may contain \\ on Windows).
    source_path:
        Resolved archive source path (archive_root / relative_path).
    destination_path:
        Resolved destination path (destination_root / relative_path).
    operation_type:
        Planned restore operation type.
    reason:
        Human-readable planning rationale.
    """

    operation_index: int
    relative_path: str
    source_path: Path
    destination_path: Path
    operation_type: RestoreOperationType
    reason: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable payload."""
        return {
            "operation_index": self.operation_index,
            "relative_path": self.relative_path,
            "source_path": str(self.source_path),
            "destination_path": str(self.destination_path),
            "operation_type": self.operation_type.value,
            "reason": self.reason,
        }
