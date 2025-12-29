from __future__ import annotations

import json
from pathlib import Path

from backup_engine.restore.service import run_restore


def _write_run_manifest(path: Path, archive_root: Path) -> None:
    payload = {
        "schema_version": "wcbt_run_manifest_v2",
        "run_id": "20251229_035431Z",
        "created_at_utc": "2025-12-29T03:54:31Z",
        "archive_root": str(archive_root),
        "plan_text_path": str(archive_root / "plan.txt"),
        "profile_name": "smoke",
        "source_root": "C:\\dummy\\source",
        "scan_issues": [],
        "operations": [
            {
                "operation_type": "copy_file_to_archive",
                "relative_path": "a.txt",
                "source_path": "C:\\dummy\\source\\a.txt",
                "destination_path": str(archive_root / "a.txt"),
                "reason": "test",
            },
            {
                "operation_type": "copy_file_to_archive",
                "relative_path": "nested\\b.txt",
                "source_path": "C:\\dummy\\source\\nested\\b.txt",
                "destination_path": str(archive_root / "nested" / "b.txt"),
                "reason": "test",
            },
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_restore_plan_and_candidates_written(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)

    manifest_path = archive_root / "manifest.json"
    _write_run_manifest(manifest_path, archive_root)

    dest_root = tmp_path / "dest"
    # Must be absolute; tmp_path is absolute already, dest does not need to exist yet.
    run_restore(
        manifest_path=manifest_path,
        destination_root=dest_root,
        mode="add-only",
        verify="size",
        dry_run=True,
        data_root=None,
    )

    artifacts_root = dest_root / ".wcbt_restore" / "20251229_035431Z"
    restore_plan_path = artifacts_root / "restore_plan.json"
    restore_candidates_path = artifacts_root / "restore_candidates.jsonl"

    assert restore_plan_path.exists()
    assert restore_candidates_path.exists()

    plan_payload = json.loads(restore_plan_path.read_text(encoding="utf-8"))
    assert plan_payload["schema_version"] == "wcbt_restore_plan_v1"
    assert plan_payload["execution_strategy"] == "staged_atomic_replace"
    assert plan_payload["run_id"] == "20251229_035431Z"

    lines = restore_candidates_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    c0 = json.loads(lines[0])
    c1 = json.loads(lines[1])

    assert c0["operation_index"] == 0
    assert c0["relative_path"] == "a.txt"
    assert c1["operation_index"] == 1
    assert c1["relative_path"] == "nested\\b.txt"

    assert c0["destination_path"].endswith(str(dest_root / "a.txt"))
    assert c1["destination_path"].endswith(str(dest_root / "nested" / "b.txt"))


def test_restore_materialize_marks_existing_as_skip_in_add_only(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)

    manifest_path = archive_root / "manifest.json"
    _write_run_manifest(manifest_path, archive_root)

    dest_root = tmp_path / "dest"
    (dest_root / "nested").mkdir(parents=True, exist_ok=True)
    (dest_root / "nested" / "b.txt").write_text("existing", encoding="utf-8")

    run_restore(
        manifest_path=manifest_path,
        destination_root=dest_root,
        mode="add-only",
        verify="size",
        dry_run=True,
        data_root=None,
    )

    artifacts_root = dest_root / ".wcbt_restore" / "20251229_035431Z"
    restore_candidates_path = artifacts_root / "restore_candidates.jsonl"
    lines = restore_candidates_path.read_text(encoding="utf-8").splitlines()
    c1 = json.loads(lines[1])
    assert c1["operation_type"] == "skip_existing"


def test_restore_materialize_marks_existing_as_overwrite_in_overwrite_mode(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)

    manifest_path = archive_root / "manifest.json"
    _write_run_manifest(manifest_path, archive_root)

    dest_root = tmp_path / "dest"
    (dest_root / "nested").mkdir(parents=True, exist_ok=True)
    (dest_root / "nested" / "b.txt").write_text("existing", encoding="utf-8")

    run_restore(
        manifest_path=manifest_path,
        destination_root=dest_root,
        mode="overwrite",
        verify="size",
        dry_run=True,
        data_root=None,
    )

    artifacts_root = dest_root / ".wcbt_restore" / "20251229_035431Z"
    restore_candidates_path = artifacts_root / "restore_candidates.jsonl"
    lines = restore_candidates_path.read_text(encoding="utf-8").splitlines()
    c1 = json.loads(lines[1])
    assert c1["operation_type"] == "overwrite_existing"
