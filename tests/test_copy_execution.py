from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backup_engine.backup.service import run_backup
from backup_engine.clock import FixedClock
from backup_engine.paths_and_safety import resolve_profile_paths
from backup_engine.errors import BackupExecutionError


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_execute_copies_files_and_records_results(tmp_path: Path) -> None:
    profile_name = "test_profile"
    paths = resolve_profile_paths(profile_name, data_root=tmp_path)

    source_root = tmp_path / "source"
    (source_root / "nested").mkdir(parents=True)
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")
    (source_root / "nested" / "b.txt").write_text("bravo", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=tmp_path,
        max_items=100,
        write_plan=False,
        clock=clock,
        execute=True,
    )

    run_root = paths.archives_root / "20250101_000000Z"
    assert run_root.is_dir()
    assert (run_root / "plan.txt").is_file()
    assert (run_root / "manifest.json").is_file()

    assert (run_root / "a.txt").read_text(encoding="utf-8") == "alpha"
    assert (run_root / "nested" / "b.txt").read_text(encoding="utf-8") == "bravo"

    manifest = _read_json(run_root / "manifest.json")
    assert manifest["schema_version"] == "wcbt_run_manifest_v2"
    assert manifest["run_id"] == "20250101_000000Z"
    assert manifest["execution"]["status"] == "success"
    assert len(manifest["execution"]["results"]) == len(manifest["operations"])


def test_execute_fails_fast_on_reserved_destination_collision(tmp_path: Path) -> None:
    profile_name = "test_profile"
    paths = resolve_profile_paths(profile_name, data_root=tmp_path)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "plan.txt").write_text("I am a source file named plan.txt", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    with pytest.raises(BackupExecutionError):
        run_backup(
            profile_name=profile_name,
            source=source_root,
            dry_run=False,
            data_root=tmp_path,
            max_items=100,
            write_plan=False,
            clock=clock,
            execute=True,
        )

    run_root = paths.archives_root / "20250101_000000Z"
    assert run_root.is_dir()
    assert (run_root / "plan.txt").is_file()
    assert (run_root / "manifest.json").is_file()

    manifest = _read_json(run_root / "manifest.json")
    assert manifest["execution"]["status"] == "failed"

    artifact_text = (run_root / "plan.txt").read_text(encoding="utf-8")
    assert "Profile" in artifact_text
