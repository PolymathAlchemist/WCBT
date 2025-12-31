from __future__ import annotations

import json
from pathlib import Path

import pytest

from backup_engine.restore.data_models import RestoreMode, RestorePlan, RestoreVerification
from backup_engine.restore.errors import RestoreArtifactError
from backup_engine.restore.materialize import materialize_restore_candidates
from backup_engine.restore.service import run_restore


def _make_plan(
    *,
    archive_root: Path,
    destination_root: Path,
    mode: RestoreMode,
) -> RestorePlan:
    return RestorePlan(
        schema_version="wcbt_restore_plan_v1",
        execution_strategy="staged_atomic_replace",
        run_id="run_test",
        created_at_utc="2025-01-01T00:00:00Z",
        manifest_path=Path("run_manifest.json"),
        archive_root=archive_root,
        destination_root=destination_root,
        profile_name="profile",
        mode=mode,
        verification=RestoreVerification("none"),
        source_manifest={
            "schema_version": "wcbt_run_manifest_v2",
            "run_id": "run_test",
            "created_at_utc": "2025-01-01T00:00:00Z",
            "archive_root": str(archive_root),
            "profile_name": "profile",
            "source_root": "",
            "plan_text_path": "",
            "manifest_sha256": "x" * 64,
            "operations_count": 1,
            "_operations_full": [{"relative_path": "a.txt"}],
        },
    )


def test_restore_add_only_conflict_fails_with_artifact(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    dest = tmp_path / "dest"
    archive.mkdir()
    dest.mkdir()

    # Simulate existing destination file
    (dest / "a.txt").write_text("existing", encoding="utf-8")

    manifest = tmp_path / "run_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "wcbt_run_manifest_v2",
                "run_id": "run1",
                "created_at_utc": "2025-01-01T00:00:00Z",
                "archive_root": str(archive),
                "profile_name": "profile",
                "operations": [{"relative_path": "a.txt"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(RestoreArtifactError):
        run_restore(
            manifest_path=manifest,
            destination_root=dest,
            mode="add-only",
            verify="none",
            dry_run=True,
        )

    conflicts = dest / ".wcbt_restore" / "run1" / "restore_conflicts.jsonl"
    assert conflicts.is_file()


def test_materialize_overwrite_mode_allows_existing_destination(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    destination_root = tmp_path / "dest"
    archive_root.mkdir()
    destination_root.mkdir()

    (destination_root / "a.txt").write_text("existing", encoding="utf-8")

    plan = _make_plan(
        archive_root=archive_root,
        destination_root=destination_root,
        mode=RestoreMode("overwrite"),
    )

    candidates = materialize_restore_candidates(plan)
    assert len(candidates) == 1
    assert candidates[0].operation_type.value == "overwrite_existing"
