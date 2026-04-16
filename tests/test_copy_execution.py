from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest

import backup_engine.backup.service as backup_service
from backup_engine.backup.service import run_backup
from backup_engine.clock import FixedClock
from backup_engine.errors import BackupExecutionError
from backup_engine.paths_and_safety import resolve_profile_paths


def _expect_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AssertionError("Expected a JSON object (mapping).")
    return value


def _expect_sequence(value: object) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise AssertionError("Expected a JSON array (sequence).")
    return value


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("Expected manifest payload to be a JSON object")
    return cast(dict[str, object], payload)


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
    execution = _expect_mapping(manifest["execution"])
    assert execution["status"] == "success"

    results = _expect_sequence(execution["results"])
    operations = _expect_sequence(manifest["operations"])

    assert execution["status"] == "success"
    assert len(results) == len(operations)


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
    execution = _expect_mapping(manifest["execution"])
    assert execution["status"] == "failed"

    artifact_text = (run_root / "plan.txt").read_text(encoding="utf-8")
    assert "Profile" in artifact_text


def test_compressed_backup_preserves_staging_when_archive_creation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile_name = "test_profile"
    paths = resolve_profile_paths(profile_name, data_root=tmp_path)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    def _raise_archive_failure(**_: object) -> object:
        raise OSError("zip failed")

    monkeypatch.setattr("backup_engine.compression.compress_run_directory", _raise_archive_failure)

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
            compress=True,
            compression="zip",
            job_name="photos",
        )

    staging_root = paths.work_root / "oz0_staging" / "photos.20250101_000000Z"
    assert staging_root.is_dir()
    assert (staging_root / "a.txt").is_file()

    oz0_root = source_root.parent / "OZ0"
    assert not oz0_root.exists()


def test_compressed_backup_keeps_archive_when_manifest_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile_name = "test_profile"
    paths = resolve_profile_paths(profile_name, data_root=tmp_path)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    def _raise_manifest_failure(*_: object, **__: object) -> None:
        raise OSError("manifest failed")

    monkeypatch.setattr(backup_service, "write_run_manifest_atomic", _raise_manifest_failure)

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=tmp_path,
        max_items=100,
        write_plan=False,
        clock=clock,
        execute=True,
        compress=True,
        compression="zip",
        job_name="photos",
    )

    archive_path = source_root.parent / "OZ0" / "photos.20250101_000000Z.OZ0.zip"
    assert archive_path.is_file()
    assert result.manifest_path is None

    staging_root = paths.work_root / "oz0_staging"
    assert (not staging_root.exists()) or (not any(staging_root.iterdir()))


def test_compressed_backup_report_distinguishes_staging_and_artifact_root(tmp_path: Path) -> None:
    profile_name = "test_profile"
    paths = resolve_profile_paths(profile_name, data_root=tmp_path)

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=tmp_path,
        max_items=100,
        write_plan=False,
        clock=clock,
        execute=True,
        compress=True,
        compression="zip",
        job_name="Minecraft",
    )

    assert "Staging dir :" in result.report_text
    assert str(paths.work_root / "oz0_staging" / "Minecraft.20250101_000000Z") in result.report_text
    assert "Artifact root:" in result.report_text
    assert str(source_root.parent / "OZ0") in result.report_text
    assert "Archive root: " not in result.report_text


def test_compressed_plan_mode_uses_same_oz0_artifact_root_as_execution(tmp_path: Path) -> None:
    profile_name = "test_profile"

    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=True,
        data_root=tmp_path / "data_root",
        max_items=100,
        write_plan=True,
        clock=clock,
        compression="zip",
        job_name="Minecraft",
    )

    expected_oz0_root = source_root.parent / "OZ0"
    assert result.archive_root == expected_oz0_root
    assert result.plan_text_path == expected_oz0_root / "plan.txt"
    assert result.plan_text_path is not None and result.plan_text_path.is_file()
    assert "Artifact root:" in result.report_text
    assert str(expected_oz0_root) in result.report_text
    assert "Staging dir :" not in result.report_text


def test_uncompressed_plan_mode_writes_plan_to_oz0_root(tmp_path: Path) -> None:
    profile_name = "test_profile"

    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=True,
        data_root=tmp_path / "data_root",
        max_items=100,
        write_plan=True,
        clock=clock,
        compression="none",
        job_name="Minecraft",
    )

    expected_oz0_root = source_root.parent / "OZ0"
    assert result.archive_root == expected_oz0_root
    assert result.plan_text_path == expected_oz0_root / "plan.txt"
    assert result.plan_text_path is not None and result.plan_text_path.is_file()
    assert f"Archive root: {expected_oz0_root}" in result.report_text
    assert str(expected_oz0_root) in result.plan_text_path.read_text(encoding="utf-8")
