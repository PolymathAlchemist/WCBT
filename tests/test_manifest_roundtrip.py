from __future__ import annotations

from pathlib import Path
from uuid import UUID

from backup_engine.data_models import (
    ArchiveFormat,
    ArchiveInfo,
    BackupManifest,
    BackupMetadata,
    DedicatedServerSource,
    EnvironmentInfo,
    SourceType,
    utc_now,
)
from backup_engine.manifest_store import (
    BackupRunManifestV2,
    read_manifest,
    read_manifest_json,
    write_manifest_atomic,
    write_run_manifest_atomic,
)


def test_manifest_round_trip(tmp_path: Path) -> None:
    backup_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    world_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    env = EnvironmentInfo(
        minecraft_version="1.20.4",
        loader="fabric",
        loader_version="0.15.11",
        java_version="17.0.9",
    )
    src = DedicatedServerSource(
        type=SourceType.DEDICATED_SERVER,
        server_root="C:/servers/mc",
        world_folder="world",
    )
    arc = ArchiveInfo(
        format=ArchiveFormat.TAR_ZST, filename="World_2025-01-01_00-00-00_x.tar.zst", size_bytes=123
    )

    manifest = BackupManifest.new(
        backup_id=backup_id,
        world_id=world_id,
        created_at_utc=utc_now(),
        profile_name="Default",
        environment=env,
        source=src,
        archive=arc,
        metadata=BackupMetadata(tags=["daily"], note="test", pinned=True),
        telemetry={"example": 1},
    )

    path = tmp_path / "manifest.json"
    write_manifest_atomic(path, manifest)
    loaded = read_manifest(path)

    assert loaded.backup_id == manifest.backup_id
    assert loaded.world_id == manifest.world_id
    assert loaded.archive.format == ArchiveFormat.TAR_ZST
    assert loaded.metadata.pinned is True
    assert loaded.telemetry["example"] == 1


def test_backup_run_manifest_round_trip_preserves_backup_origin(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = BackupRunManifestV2(
        schema_version=BackupRunManifestV2.SCHEMA_VERSION,
        run_id="20260101_000000Z",
        created_at_utc="2026-01-01T00:00:00Z",
        archive_root=str(tmp_path / "archive"),
        plan_text_path=str(tmp_path / "archive" / "plan.txt"),
        profile_name="default",
        source_root="C:/source",
        backup_origin="pre_restore",
        backup_note="Pre-restore safety backup executed by restore workflow",
        archive_format="zip",
        compression_method="deflate",
        compression_level=None,
        archive_writer_version=None,
        archive_extension=".zip",
        operations=[],
        scan_issues=[],
    )

    write_run_manifest_atomic(manifest_path, manifest)
    loaded = BackupRunManifestV2.from_dict(read_manifest_json(manifest_path))

    assert loaded.backup_origin == "pre_restore"
    assert loaded.backup_note == "Pre-restore safety backup executed by restore workflow"
    assert loaded.archive_format == "zip"
    assert loaded.compression_method == "deflate"
    assert loaded.compression_level is None
    assert loaded.archive_writer_version is None
    assert loaded.archive_extension == ".zip"


def test_backup_run_manifest_round_trip_allows_missing_backup_origin(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = BackupRunManifestV2(
        schema_version=BackupRunManifestV2.SCHEMA_VERSION,
        run_id="20260101_000000Z",
        created_at_utc="2026-01-01T00:00:00Z",
        archive_root=str(tmp_path / "archive"),
        plan_text_path=str(tmp_path / "archive" / "plan.txt"),
        profile_name="default",
        source_root="C:/source",
        operations=[],
        scan_issues=[],
    )

    write_run_manifest_atomic(manifest_path, manifest)
    payload = read_manifest_json(manifest_path)
    loaded = BackupRunManifestV2.from_dict(payload)

    assert "backup_origin" not in payload
    assert "backup_note" not in payload
    assert payload["archive_format"] is None
    assert payload["compression_method"] is None
    assert payload["compression_level"] is None
    assert payload["archive_writer_version"] is None
    assert payload["archive_extension"] is None
    assert loaded.backup_origin is None
    assert loaded.backup_note is None
    assert loaded.archive_format is None
    assert loaded.compression_method is None
    assert loaded.compression_level is None
    assert loaded.archive_writer_version is None
    assert loaded.archive_extension is None


def test_backup_run_manifest_from_dict_allows_missing_archive_metadata_fields() -> None:
    loaded = BackupRunManifestV2.from_dict(
        {
            "schema_version": BackupRunManifestV2.SCHEMA_VERSION,
            "run_id": "20260101_000000Z",
            "created_at_utc": "2026-01-01T00:00:00Z",
            "archive_root": "C:/archive",
            "plan_text_path": "C:/archive/plan.txt",
            "profile_name": "default",
            "source_root": "C:/source",
            "operations": [],
            "scan_issues": [],
        }
    )

    assert loaded.archive_format is None
    assert loaded.compression_method is None
    assert loaded.compression_level is None
    assert loaded.archive_writer_version is None
    assert loaded.archive_extension is None
