from __future__ import annotations

from pathlib import Path

from backup_engine.init_profile import init_profile


def test_init_profile_creates_expected_directories(tmp_path: Path) -> None:
    paths = init_profile(profile_name="default", data_root=tmp_path)

    assert paths.profile_root.is_dir()
    assert paths.manifests_root.is_dir()
    assert paths.archives_root.is_dir()
    assert paths.index_root.is_dir()
    assert paths.logs_root.is_dir()
    assert paths.work_root.is_dir()
    assert paths.live_snapshots_root.is_dir()

    for directory in (
        paths.profile_root,
        paths.manifests_root,
        paths.archives_root,
        paths.index_root,
        paths.logs_root,
        paths.work_root,
        paths.live_snapshots_root,
    ):
        assert tmp_path in directory.parents or directory == tmp_path
