from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from backup_engine.manifest_store import read_manifest_json
from backup_engine.paths_and_safety import SafetyViolationError

from .data_models import RestoreIntent, RestoreMode, RestorePlan, RestoreVerification
from .errors import RestoreIntentError, RestoreManifestError, RestoreSafetyError

_RESTORE_PLAN_SCHEMA_VERSION = "wcbt_restore_plan_v1"
_EXECUTION_STRATEGY = "staged_atomic_replace"
_EXPECTED_RUN_MANIFEST_SCHEMA = "wcbt_run_manifest_v2"


def _sha256_text(text: str) -> str:
    digest = hashlib.sha256()
    digest.update(text.encode("utf-8"))
    return digest.hexdigest()


def _read_manifest_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RestoreManifestError(f"Failed to read manifest text: {path}") from exc


def _validate_destination_root(destination_root: Path) -> None:
    if str(destination_root).strip() == "":
        raise RestoreIntentError("Destination root must be a non-empty path.")

    if not destination_root.is_absolute():
        raise RestoreIntentError(f"Destination root must be an absolute path: {destination_root}")

    # We will create artifact folders under destination_root. It must not be a file.
    if destination_root.exists() and not destination_root.is_dir():
        raise RestoreIntentError(f"Destination root exists but is not a directory: {destination_root}")


def build_restore_plan(intent: RestoreIntent) -> RestorePlan:
    """
    Build a deterministic restore plan from a run manifest.

    Parameters
    ----------
    intent:
        Restore intent.

    Returns
    -------
    RestorePlan
        Deterministic restore plan object.

    Raises
    ------
    RestoreManifestError
        If the manifest is missing required fields or schema tag.
    RestoreIntentError
        If destination root violates invariants.
    RestoreSafetyError
        If safety constraints are violated.
    """
    _validate_destination_root(intent.destination_root)

    payload = read_manifest_json(intent.manifest_path)

    schema_version = payload.get("schema_version")
    if schema_version != _EXPECTED_RUN_MANIFEST_SCHEMA:
        raise RestoreManifestError(
            f"Unsupported run manifest schema_version: {schema_version!r} "
            f"(expected {_EXPECTED_RUN_MANIFEST_SCHEMA!r})"
        )

    required_keys = [
        "run_id",
        "created_at_utc",
        "archive_root",
        "profile_name",
        "operations",
    ]
    missing = [k for k in required_keys if k not in payload]
    if missing:
        raise RestoreManifestError(f"Run manifest missing required keys: {missing}")

    archive_root = Path(str(payload["archive_root"])).expanduser()
    if not archive_root.exists() or not archive_root.is_dir():
        raise RestoreManifestError(f"archive_root does not exist or is not a directory: {archive_root}")

    destination_root = intent.destination_root.expanduser()

    # Safety: refuse obviously dangerous overlap scenarios.
    # Note: On Windows, Path comparisons are typically case-insensitive; resolve() helps.
    try:
        archive_resolved = archive_root.resolve()
        dest_resolved = destination_root.resolve()
    except OSError:
        # If resolve fails (e.g., dest doesn't exist yet), keep conservative checks.
        archive_resolved = archive_root
        dest_resolved = destination_root

    def _is_within(child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    if _is_within(dest_resolved, archive_resolved) or _is_within(archive_resolved, dest_resolved):
        raise RestoreSafetyError(
            f"Destination root and archive root must not contain each other. "
            f"dest={destination_root} archive={archive_root}"
        )

    # Also enforce existing SafetyViolationError posture if needed by callers.
    # Here we raise RestoreSafetyError, but we preserve the external catch behavior via CLI.
    try:
        # This is a "no-op" use of the imported type so tools don't prune it; also signals posture.
        _ = SafetyViolationError
    except Exception:
        pass

    run_id = str(payload["run_id"])
    created_at_utc = str(payload["created_at_utc"])
    profile_name = str(payload["profile_name"])

    # Record a stable checksum of the raw manifest text for later safety/audit.
    manifest_text = _read_manifest_text(intent.manifest_path)
    manifest_sha256 = _sha256_text(manifest_text)

    # Keep only minimum fields needed for deterministic restore planning.
    source_manifest_min: dict[str, Any] = {
        "schema_version": schema_version,
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "archive_root": str(payload["archive_root"]),
        "profile_name": profile_name,
        "source_root": str(payload.get("source_root", "")),
        "plan_text_path": str(payload.get("plan_text_path", "")),
        "manifest_sha256": manifest_sha256,
        "operations_count": len(payload.get("operations", [])) if isinstance(payload.get("operations"), list) else 0,
    }

    return RestorePlan(
        schema_version=_RESTORE_PLAN_SCHEMA_VERSION,
        execution_strategy=_EXECUTION_STRATEGY,
        run_id=run_id,
        created_at_utc=created_at_utc,
        manifest_path=intent.manifest_path,
        archive_root=archive_root,
        destination_root=destination_root,
        profile_name=profile_name,
        mode=intent.mode,
        verification=intent.verification,
        source_manifest=source_manifest_min,
    )


def parse_restore_mode(value: str) -> RestoreMode:
    """Parse CLI restore mode string into RestoreMode."""
    try:
        return RestoreMode(value)
    except ValueError as exc:
        raise RestoreIntentError(f"Invalid restore mode: {value!r}") from exc


def parse_restore_verification(value: str) -> RestoreVerification:
    """Parse CLI verification string into RestoreVerification."""
    try:
        return RestoreVerification(value)
    except ValueError as exc:
        raise RestoreIntentError(f"Invalid verification level: {value!r}") from exc
