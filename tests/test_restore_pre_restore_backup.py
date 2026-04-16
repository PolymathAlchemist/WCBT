from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest

from backup_engine.backup.service import BackupRunResult
from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.restore.errors import RestoreManifestError
from backup_engine.restore.journal import Clock
from backup_engine.restore.service import run_restore


@dataclass(frozen=True)
class FixedClock(Clock):
    now_value: datetime

    def now(self) -> datetime:
        return self.now_value


def _write_restore_manifest(
    manifest_path: Path,
    archive_root: Path,
    *,
    job_id: str | None = None,
    job_name: str | None = None,
    source_root: str = "C:\\historical\\source",
    archive_relative_path: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "schema_version": "wcbt_run_manifest_v2",
        "run_id": "restore-run",
        "created_at_utc": "2025-01-01T00:00:00Z",
        "archive_root": str(archive_root),
        "profile_name": "default",
        "source_root": source_root,
        "operations": [
            {
                "relative_path": "world.dat",
            }
        ],
    }
    if job_id is not None:
        payload["job_id"] = job_id
    if job_name is not None:
        payload["job_name"] = job_name
    if archive_relative_path is not None:
        payload["archive"] = {
            "format": "zip",
            "relative_path": archive_relative_path,
            "size_bytes": 123,
            "sha256": "a" * 64,
        }

    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _dummy_backup_result(root: Path) -> BackupRunResult:
    return BackupRunResult(
        run_id="backup-run",
        profile_name="default",
        source_root=root,
        archive_root=root.parent / "OZ0",
        dry_run=False,
        report_text="ok",
        plan_text_path=None,
        manifest_path=(root.parent / "OZ0" / "backup-run.manifest.json"),
        executed=True,
    )


def test_non_dry_run_restore_performs_pre_restore_backup_before_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    (archive_root / "world.dat").write_text("restored", encoding="utf-8")
    manifest_path = tmp_path / "restore_manifest.json"
    _write_restore_manifest(manifest_path, archive_root)

    destination_root = tmp_path / "live_world"
    destination_root.mkdir()
    (destination_root / "world.dat").write_text("current-live", encoding="utf-8")

    observed_calls: list[Path] = []

    def _run_backup(**kwargs: object) -> BackupRunResult:
        observed_calls.append(Path(cast(Path, kwargs["source"])))
        assert (destination_root / "world.dat").read_text(encoding="utf-8") == "current-live"
        return _dummy_backup_result(destination_root)

    monkeypatch.setattr("backup_engine.restore.service.run_backup", _run_backup)

    run_restore(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode="overwrite",
        verify="none",
        dry_run=False,
        data_root=tmp_path / "data_root",
    )

    assert observed_calls == [destination_root]
    assert (destination_root / "world.dat").read_text(encoding="utf-8") == "restored"


def test_pre_restore_backup_failure_aborts_restore_without_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    (archive_root / "world.dat").write_text("restored", encoding="utf-8")
    manifest_path = tmp_path / "restore_manifest.json"
    _write_restore_manifest(manifest_path, archive_root)

    destination_root = tmp_path / "live_world"
    destination_root.mkdir()
    (destination_root / "world.dat").write_text("current-live", encoding="utf-8")

    def _run_backup(**_: object) -> BackupRunResult:
        raise RuntimeError("pre-restore backup failed")

    monkeypatch.setattr("backup_engine.restore.service.run_backup", _run_backup)

    with pytest.raises(RuntimeError, match="pre-restore backup failed"):
        run_restore(
            manifest_path=manifest_path,
            destination_root=destination_root,
            mode="overwrite",
            verify="none",
            dry_run=False,
            data_root=tmp_path / "data_root",
        )

    assert (destination_root / "world.dat").read_text(encoding="utf-8") == "current-live"
    assert not (destination_root / ".wcbt_restore").exists()


def test_pre_restore_backup_uses_current_live_job_inputs_not_historical_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_root = tmp_path / "data_root"
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    (archive_root / "world.dat").write_text("restored", encoding="utf-8")
    manifest_path = tmp_path / "restore_manifest.json"
    _write_restore_manifest(
        manifest_path,
        archive_root,
        job_id="historical-job-id",
        job_name="Historical Job",
        source_root="C:\\historical\\world",
    )

    destination_root = tmp_path / "live_world"
    destination_root.mkdir()
    (destination_root / "world.dat").write_text("current-live", encoding="utf-8")

    store = open_profile_store(profile_name="default", data_root=data_root)
    live_job_id = store.create_job("Current Live Job")
    live_binding = store.load_job_binding(live_job_id)
    store.save_job_binding(
        JobBinding(
            job_id=live_binding.job_id,
            job_name="Current Live Job",
            template_id=live_binding.template_id,
            source_root=str(destination_root),
        )
    )
    store.save_template_compression(job_id=live_job_id, name="Current Live Job", compression="zip")

    recorded_kwargs: dict[str, object] = {}

    def _run_backup(**kwargs: object) -> BackupRunResult:
        recorded_kwargs.update(kwargs)
        return _dummy_backup_result(destination_root)

    monkeypatch.setattr("backup_engine.restore.service.run_backup", _run_backup)

    run_restore(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode="overwrite",
        verify="none",
        dry_run=False,
        data_root=data_root,
    )

    assert cast(Path, recorded_kwargs["source"]) == destination_root
    assert recorded_kwargs["job_id"] == live_job_id
    assert recorded_kwargs["job_name"] == "Current Live Job"
    assert recorded_kwargs["compression"] == "zip"


def test_pre_restore_backup_with_oz0_backed_live_job_writes_normal_oz0_artifacts(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data_root"
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    (archive_root / "world.dat").write_text("restored", encoding="utf-8")
    manifest_path = tmp_path / "restore_manifest.json"
    _write_restore_manifest(manifest_path, archive_root)

    destination_root = tmp_path / "live_world"
    destination_root.mkdir()
    (destination_root / "world.dat").write_text("current-live", encoding="utf-8")

    store = open_profile_store(profile_name="default", data_root=data_root)
    live_job_id = store.create_job("Minecraft")
    live_binding = store.load_job_binding(live_job_id)
    store.save_job_binding(
        JobBinding(
            job_id=live_binding.job_id,
            job_name="Minecraft",
            template_id=live_binding.template_id,
            source_root=str(destination_root),
        )
    )
    store.save_template_compression(job_id=live_job_id, name="Minecraft", compression="zip")

    clock = FixedClock(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

    run_restore(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode="overwrite",
        verify="none",
        dry_run=False,
        data_root=data_root,
        clock=clock,
    )

    oz0_root = destination_root.parent / "OZ0"
    assert (oz0_root / "Minecraft.20250101_000000Z.OZ0.zip").is_file()
    assert (oz0_root / "Minecraft.20250101_000000Z.manifest.json").is_file()


def test_restore_still_works_when_current_live_job_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    (archive_root / "world.dat").write_text("restored", encoding="utf-8")
    manifest_path = tmp_path / "restore_manifest.json"
    _write_restore_manifest(
        manifest_path,
        archive_root,
        job_id="missing-job-id",
        job_name="Missing Job",
    )

    destination_root = tmp_path / "live_world"
    destination_root.mkdir()
    (destination_root / "world.dat").write_text("current-live", encoding="utf-8")

    def _run_backup(**kwargs: object) -> BackupRunResult:
        assert kwargs["job_id"] is None
        return _dummy_backup_result(destination_root)

    monkeypatch.setattr("backup_engine.restore.service.run_backup", _run_backup)

    run_restore(
        manifest_path=manifest_path,
        destination_root=destination_root,
        mode="overwrite",
        verify="none",
        dry_run=False,
        data_root=tmp_path / "data_root",
    )

    assert (destination_root / "world.dat").read_text(encoding="utf-8") == "restored"


def test_manifest_with_missing_archive_metadata_fails_explicitly(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    manifest_path = tmp_path / "restore_manifest.json"
    _write_restore_manifest(
        manifest_path,
        archive_root,
        archive_relative_path="missing-backup.zip",
    )

    destination_root = tmp_path / "restore_destination"

    with pytest.raises(RestoreManifestError, match="archive could not be found"):
        run_restore(
            manifest_path=manifest_path,
            destination_root=destination_root,
            mode="overwrite",
            verify="none",
            dry_run=True,
            data_root=None,
        )
