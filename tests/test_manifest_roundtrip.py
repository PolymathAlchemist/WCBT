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
from backup_engine.manifest_store import read_manifest, write_manifest_atomic


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
