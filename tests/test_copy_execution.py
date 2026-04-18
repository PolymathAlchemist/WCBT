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
from backup_engine.job_binding import JobBinding
from backup_engine.paths_and_safety import resolve_profile_paths
from backup_engine.profile_store.sqlite_store import open_profile_store


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


def _create_job_binding(
    *, profile_name: str, data_root: Path, source_root: Path
) -> tuple[str, str]:
    store = open_profile_store(profile_name=profile_name, data_root=data_root)
    job_name = "Minecraft"
    job_id = store.create_job(job_name)
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name=job_name,
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    return job_id, job_name


def test_execute_copies_files_and_records_results(tmp_path: Path) -> None:
    profile_name = "test_profile"
    paths = resolve_profile_paths(profile_name, data_root=tmp_path)

    source_root = tmp_path / "source"
    (source_root / "nested").mkdir(parents=True)
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")
    (source_root / "nested" / "b.txt").write_text("bravo", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    _create_job_binding(profile_name=profile_name, data_root=tmp_path, source_root=source_root)

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=tmp_path,
        max_items=100,
        write_plan=False,
        clock=clock,
        execute=True,
    )

    expected_oz0_root = source_root.parent / "source.OZ0"
    run_root = expected_oz0_root / "20250101_000000Z"
    assert result.archive_root == expected_oz0_root
    assert str(paths.archives_root) not in result.report_text
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

    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "plan.txt").write_text("I am a source file named plan.txt", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    _create_job_binding(profile_name=profile_name, data_root=tmp_path, source_root=source_root)

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

    run_root = source_root.parent / "source.OZ0" / "20250101_000000Z"
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

    oz0_root = source_root.parent / "source.OZ0"
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

    archive_path = source_root.parent / "source.OZ0" / "photos.20250101_000000Z.OZ0.zip"
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
    assert str(source_root.parent / "source.OZ0") in result.report_text
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

    expected_oz0_root = source_root.parent / "testing.OZ0"
    expected_plan_path = expected_oz0_root / "plan_20250101_000000Z.txt"
    assert result.archive_root == expected_oz0_root
    assert result.plan_text_path == expected_plan_path
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

    expected_oz0_root = source_root.parent / "testing.OZ0"
    expected_plan_path = expected_oz0_root / "plan_20250101_000000Z.txt"
    assert result.archive_root == expected_oz0_root
    assert result.plan_text_path == expected_plan_path
    assert result.plan_text_path is not None and result.plan_text_path.is_file()
    assert f"Archive root: {expected_oz0_root}" in result.report_text
    assert str(expected_oz0_root) in result.plan_text_path.read_text(encoding="utf-8")


def test_dry_run_plan_mode_uses_unique_run_tokenized_plan_paths_across_repeated_runs(
    tmp_path: Path,
) -> None:
    profile_name = "test_profile"
    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    first_result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=True,
        data_root=data_root,
        max_items=100,
        write_plan=True,
        clock=FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
        compression="none",
        job_name="Minecraft",
    )
    second_result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=True,
        data_root=data_root,
        max_items=100,
        write_plan=True,
        clock=FixedClock(datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)),
        compression="none",
        job_name="Minecraft",
    )

    expected_oz0_root = source_root.parent / "testing.OZ0"
    first_plan_path = expected_oz0_root / "plan_20250101_000000Z.txt"
    second_plan_path = expected_oz0_root / "plan_20250101_000001Z.txt"

    assert first_result.plan_text_path == first_plan_path
    assert second_result.plan_text_path == second_plan_path
    assert first_plan_path.is_file()
    assert second_plan_path.is_file()
    assert first_plan_path.read_text(encoding="utf-8") == first_result.report_text
    assert second_plan_path.read_text(encoding="utf-8") == second_result.report_text


@pytest.mark.parametrize(
    ("dry_run", "execute", "compress", "compression", "expected_plan_path"),
    [
        (True, False, False, "none", "plan_20250101_000000Z.txt"),
        (False, False, False, "none", "20250101_000000Z/plan.txt"),
        (False, True, False, "none", "20250101_000000Z/plan.txt"),
        (False, True, True, "zip", None),
    ],
)
def test_all_backup_modes_use_target_relative_oz0_root(
    tmp_path: Path,
    dry_run: bool,
    execute: bool,
    compress: bool,
    compression: str,
    expected_plan_path: str | None,
) -> None:
    profile_name = "test_profile"
    data_root = tmp_path / "data_root"
    paths = resolve_profile_paths(profile_name, data_root=data_root)

    source_root = tmp_path / "testing"
    source_root.mkdir()
    (source_root / "a.txt").write_text("alpha", encoding="utf-8")

    job_id, job_name = _create_job_binding(
        profile_name=profile_name,
        data_root=data_root,
        source_root=source_root,
    )
    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=dry_run,
        data_root=data_root,
        max_items=100,
        write_plan=dry_run,
        clock=clock,
        execute=execute,
        compress=compress,
        compression=compression,
        force=True,
        break_lock=True,
        job_id=job_id,
        job_name=job_name,
    )

    expected_oz0_root = source_root.parent / "testing.OZ0"
    assert result.archive_root == expected_oz0_root
    assert not str(result.archive_root).startswith(str(paths.archives_root))
    assert str(paths.archives_root) not in result.report_text

    if expected_plan_path is not None:
        expected_plan = expected_oz0_root / Path(expected_plan_path)
        assert result.plan_text_path == expected_plan
        assert expected_plan.is_file()
        if dry_run:
            assert result.manifest_path is None
        else:
            assert result.manifest_path == expected_plan.parent / "manifest.json"
            assert result.manifest_path is not None and result.manifest_path.is_file()
    else:
        expected_archive = expected_oz0_root / "Minecraft.20250101_000000Z.OZ0.zip"
        expected_manifest = expected_oz0_root / "Minecraft.20250101_000000Z.manifest.json"
        assert result.plan_text_path is None
        assert result.manifest_path == expected_manifest
        assert expected_archive.is_file()
        assert expected_manifest.is_file()


def test_backup_excludes_restore_transient_residue_but_keeps_user_files(tmp_path: Path) -> None:
    profile_name = "test_profile"

    source_root = tmp_path / "source"
    (source_root / "nested").mkdir(parents=True)
    (source_root / "alpha.txt").write_text("alpha", encoding="utf-8")
    (source_root / "nested" / "beta.txt").write_text("beta", encoding="utf-8")
    restore_residue_root = source_root / ".wcbt_restore" / "restore-run"
    restore_residue_root.mkdir(parents=True)
    (restore_residue_root / "restore_summary.json").write_text("transient", encoding="utf-8")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    _create_job_binding(profile_name=profile_name, data_root=tmp_path, source_root=source_root)

    result = run_backup(
        profile_name=profile_name,
        source=source_root,
        dry_run=False,
        data_root=tmp_path,
        max_items=100,
        write_plan=False,
        clock=clock,
        execute=True,
    )

    manifest_path = result.manifest_path
    assert manifest_path is not None
    manifest = _read_json(manifest_path)
    operations = _expect_sequence(manifest["operations"])
    operation_paths = {
        str(Path(cast(str, cast(Mapping[str, object], operation)["relative_path"])).as_posix())
        for operation in operations
        if isinstance(operation, Mapping)
    }

    assert "alpha.txt" in operation_paths
    assert "nested/beta.txt" in operation_paths
    assert all(".wcbt_restore" not in path for path in operation_paths)
