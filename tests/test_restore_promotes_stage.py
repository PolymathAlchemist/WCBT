from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backup_engine.manifest_store import write_json_atomic
from backup_engine.restore.journal import Clock
from backup_engine.restore.service import run_restore


@dataclass(frozen=True)
class FixedClock(Clock):
    now_value: datetime

    def now(self) -> datetime:
        return self.now_value


def _write_minimal_run_manifest(*, manifest_path: Path, archive_root: Path) -> None:
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


def test_restore_non_dry_run_promotes_stage_to_destination(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive_root"
    archive_root.mkdir()

    manifest_path = tmp_path / "run_manifest.json"
    _write_minimal_run_manifest(manifest_path=manifest_path, archive_root=archive_root)

    destination_root = tmp_path / "restore_destination"
    destination_root.mkdir()
    sentinel = destination_root / "preexisting.txt"
    sentinel.write_text("keep", encoding="utf-8")

    run_restore(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode="add-only",
        verify="none",
        dry_run=False,
        data_root=None,
        clock=FixedClock(datetime(2000, 1, 1, tzinfo=timezone.utc)),
    )

    # Destination should now be the promoted stage and contain restore artifacts.
    restore_root = destination_root / ".wcbt_restore"
    assert restore_root.exists()
    run_dirs = [p for p in restore_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    artifacts_root = run_dirs[0]

    assert (artifacts_root / "restore_plan.json").is_file()
    assert (artifacts_root / "restore_candidates.jsonl").is_file()
    assert (artifacts_root / "execution_journal.jsonl").is_file()

    # The old destination should have been preserved under the derived previous root name.
    run_id = artifacts_root.name
    previous_root = tmp_path / f".wcbt_restore_previous_{destination_root.name}_{run_id}"
    assert previous_root.exists()
    assert (previous_root / "preexisting.txt").is_file()
