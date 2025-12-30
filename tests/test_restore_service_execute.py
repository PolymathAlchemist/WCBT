from __future__ import annotations

import json
from pathlib import Path

from backup_engine.restore.service import run_restore


def _write_run_manifest(manifest_path: Path, archive_root: Path) -> None:
    """
    Minimal v2 run manifest for restore tests.

    It includes a couple of operations pointing at archived files. The restore code
    only needs schema_version and operations to materialize candidates.
    """
    payload = {
        "schema_version": "wcbt_run_manifest_v2",
        "run_id": "test_run",
        "created_at_utc": "2025-12-29T00:00:00Z",
        "archive_root": str(archive_root),
        "profile_name": "test_profile",
        "operations": [
            {"relative_path": "a.txt", "size_bytes": 3},
            {"relative_path": "nested/b.txt", "size_bytes": 5},
        ],
    }

    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_run_restore_non_dry_run_promotes_to_destination(tmp_path: Path) -> None:
    # Arrange: archive with files
    archive_root = tmp_path / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)

    (archive_root / "a.txt").write_text("hey", encoding="utf-8")
    (archive_root / "nested").mkdir(parents=True, exist_ok=True)
    (archive_root / "nested" / "b.txt").write_text("there", encoding="utf-8")

    manifest_path = archive_root / "manifest.json"
    _write_run_manifest(manifest_path, archive_root)

    dest_root = tmp_path / "dest"

    # Act: non-dry-run with size verification
    run_restore(
        manifest_path=manifest_path,
        destination_root=dest_root,
        mode="overwrite",
        verify="size",
        dry_run=False,
        data_root=None,
    )

    # Assert: files promoted into destination
    assert (dest_root / "a.txt").read_text(encoding="utf-8") == "hey"
    assert (dest_root / "nested" / "b.txt").read_text(encoding="utf-8") == "there"

    # Assert: artifacts exist (plan/candidates)
    restore_root = dest_root / ".wcbt_restore"
    assert restore_root.exists()

    run_dirs = [p for p in restore_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1

    run_dir = run_dirs[0]
    assert (run_dir / "restore_plan.json").exists()
    assert (run_dir / "restore_candidates.jsonl").exists()
    assert (run_dir / "execution_journal.jsonl").exists()

    # Assert: stage_root removed after promotion
    assert not (run_dir / "stage_root").exists()
