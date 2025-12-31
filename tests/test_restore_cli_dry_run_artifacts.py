from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backup_engine.manifest_store import write_json_atomic
from backup_engine.restore.journal import Clock
from backup_engine.restore.service import run_restore


@dataclass(frozen=True)
class FixedClock(Clock):
    """Deterministic clock for tests."""

    now_value: datetime

    def now(self) -> datetime:
        return self.now_value


def _write_minimal_run_manifest(manifest_path: Path, archive_root: Path) -> None:
    """
    Write the smallest run manifest payload accepted by restore.service.run_restore.

    Notes
    -----
    restore.service.run_restore requires:
    - schema_version == "wcbt_run_manifest_v2"
    - archive_root must exist and be a directory
    - operations must be a list (can be empty)
    """
    payload: dict[str, Any] = {
        "schema_version": "wcbt_run_manifest_v2",
        "run_id": "dummy-run-id",
        "created_at_utc": "2000-01-01T00:00:00Z",
        "archive_root": str(archive_root),
        "profile_name": "dummy-profile",
        "operations": [],
        "scan_issues": [],
    }
    write_json_atomic(manifest_path, payload)


def test_restore_dry_run_writes_artifacts(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive_root"
    archive_root.mkdir()

    manifest_path = tmp_path / "run_manifest.json"
    _write_minimal_run_manifest(manifest_path, archive_root)

    destination_root = tmp_path / "restore_destination"

    run_restore(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode="add-only",
        verify="none",
        dry_run=True,
        data_root=None,
        clock=FixedClock(datetime(2000, 1, 1, tzinfo=timezone.utc)),
    )

    # Dry-run artifacts are written directly under destination_root/.wcbt_restore/<run_id>/...
    artifacts_parent = destination_root / ".wcbt_restore"
    assert artifacts_parent.exists()
    run_dirs = [p for p in artifacts_parent.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    artifacts_root = run_dirs[0]

    assert (artifacts_root / "restore_plan.json").is_file()
    assert (artifacts_root / "restore_candidates.jsonl").is_file()
    assert (artifacts_root / "execution_journal.jsonl").is_file()

    # Stage build is skipped, but summary artifacts still document the dry-run.
    assert (artifacts_root / "stage_copy_summary.json").is_file()
    assert not (artifacts_root / "stage_copy_results.jsonl").exists()

    # Verification is skipped, but a summary artifact documents the decision.
    assert (artifacts_root / "stage_verify_summary.json").is_file()
    assert not (artifacts_root / "stage_verify_results.jsonl").exists()

    # The plan JSON should be valid JSON and include a run_id.
    plan_payload = json.loads((artifacts_root / "restore_plan.json").read_text(encoding="utf-8"))
    assert isinstance(plan_payload, dict)
    assert "run_id" in plan_payload

    # Stage root is created even on dry-run: <dest>.wcbt_stage/<run_id>/stage_root
    stage_parent = destination_root.with_name(f"{destination_root.name}.wcbt_stage")
    assert stage_parent.exists()
    stage_run_dirs = [p for p in stage_parent.iterdir() if p.is_dir()]
    assert len(stage_run_dirs) == 1
    stage_root = stage_run_dirs[0] / "stage_root"
    assert stage_root.exists()

    # Promotion is skipped in dry-run, so destination_root must still exist.
    assert destination_root.exists()
