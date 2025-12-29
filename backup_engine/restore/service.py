from __future__ import annotations

from pathlib import Path
from typing import Any

from backup_engine.manifest_store import read_manifest_json, write_json_atomic

from .data_models import RestoreIntent
from .errors import RestoreArtifactError, RestoreManifestError
from .materialize import materialize_restore_candidates
from .plan import build_restore_plan, parse_restore_mode, parse_restore_verification


def _restore_artifacts_root(destination_root: Path, run_id: str) -> Path:
    return destination_root / ".wcbt_restore" / run_id


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    try:
        write_json_atomic(path, payload)
    except Exception as exc:  # noqa: BLE001 - domain wrapper
        raise RestoreArtifactError(f"Failed to write JSON artifact: {path}") from exc


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(__import__("json").dumps(row, sort_keys=True, ensure_ascii=False))
                handle.write("\n")
    except OSError as exc:
        raise RestoreArtifactError(f"Failed to write JSONL artifact: {path}") from exc


def run_restore(
    *,
    manifest_path: Path,
    destination_root: Path,
    mode: str,
    verify: str,
    dry_run: bool,
    data_root: Path | None = None,
) -> None:
    """
    Plan/materialize a restore run.

    Notes
    -----
    This milestone does not execute restore; it only writes inspectable artifacts.

    Parameters
    ----------
    manifest_path:
        Path to run manifest.json (schema wcbt_run_manifest_v2).
    destination_root:
        Destination root directory for the restore.
    mode:
        CLI mode string ('add-only' or 'overwrite').
    verify:
        CLI verification string ('none' or 'size').
    dry_run:
        Accepted for symmetry. Restore is always plan/materialize in this milestone.
    data_root:
        Unused in this milestone; reserved for future profile-oriented restore flows.
    """
    _ = dry_run
    _ = data_root

    run_payload = read_manifest_json(manifest_path)
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

    # Internal invariant: pass full operations list without bloating the persisted plan artifact.
    plan_dict = plan.to_dict()
    plan_dict["source_manifest"]["operations_count"] = len(operations)
    plan_dict["source_manifest"]["schema_version"] = str(run_payload.get("schema_version", ""))
    plan_dict["source_manifest"]["run_id"] = str(run_payload.get("run_id", ""))
    plan_dict["source_manifest"]["archive_root"] = str(run_payload.get("archive_root", ""))
    plan_dict["source_manifest"]["profile_name"] = str(run_payload.get("profile_name", ""))

    # For materialization, we need full ops. We keep them transient and do not persist them.
    plan_for_materialize = plan
    object.__setattr__(
        plan_for_materialize,
        "source_manifest",
        {**plan.source_manifest, "_operations_full": operations},
    )

    candidates = materialize_restore_candidates(plan_for_materialize)

    artifacts_root = _restore_artifacts_root(plan.destination_root, plan.run_id)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    restore_plan_path = artifacts_root / "restore_plan.json"
    restore_candidates_path = artifacts_root / "restore_candidates.jsonl"

    _write_json(restore_plan_path, plan_dict)
    _write_jsonl(restore_candidates_path, [c.to_dict() for c in candidates])

    print(f"Restore plan written: {restore_plan_path}")
    print(f"Restore candidates written: {restore_candidates_path}")
    print(f"Planned operations: {len(candidates)}")
