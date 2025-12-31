from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from backup_engine.manifest_store import read_manifest_json, write_json_atomic

from .data_models import RestoreIntent
from .errors import RestoreArtifactError, RestoreManifestError
from .execute import promote_stage_to_destination
from .journal import Clock, RestoreExecutionJournal
from .materialize import materialize_restore_candidates
from .plan import build_restore_plan, parse_restore_mode, parse_restore_verification
from .stage import build_restore_stage
from .verify import verify_restore_stage


def _restore_artifacts_root(destination_root: Path, run_id: str) -> Path:
    return destination_root / ".wcbt_restore" / run_id


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    try:
        write_json_atomic(path, payload)
    except Exception as exc:  # noqa: BLE001
        raise RestoreArtifactError(f"Failed to write JSON artifact: {path}") from exc


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False))
                handle.write("\n")
    except OSError as exc:
        raise RestoreArtifactError(f"Failed to write JSONL artifact: {path}") from exc


@dataclass(frozen=True)
class SystemClock(Clock):
    """Wall-clock implementation for production runs."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def run_restore(
    *,
    manifest_path: Path,
    destination_root: Path,
    mode: str,
    verify: str,
    dry_run: bool,
    data_root: Path | None = None,
    clock: Clock | None = None,
) -> None:
    """
    Plan/materialize and build a staged restore tree.

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
        If True, write plan artifacts and journal, but do not copy files into stage.
    data_root:
        Unused in this milestone; reserved for future profile-oriented restore flows.
    clock:
        Injectable clock used for deterministic journaling.
    """
    _ = data_root

    clock_to_use = clock if clock is not None else SystemClock()

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
        return

    # Important: do not write to the journal after promotion, because promotion
    # atomically replaces/renames destination_root, and this journal lives under it.
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
