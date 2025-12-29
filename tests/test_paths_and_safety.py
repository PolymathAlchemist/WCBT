from __future__ import annotations

import os
from pathlib import Path

import pytest

from backup_engine.paths_and_safety import (
    SafetyViolationError,
    default_data_root,
    ensure_profile_directories,
    resolve_profile_paths,
    validate_restore_target,
)


def test_default_data_root_prefers_local_appdata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOCALAPPDATA", r"C:\Users\TestUser\AppData\Local")
    monkeypatch.setenv("APPDATA", r"C:\Users\TestUser\AppData\Roaming")

    root = default_data_root()
    assert str(root).lower().endswith(r"appdata\local\wcbt")


def test_default_data_root_falls_back_to_appdata(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))

    root = default_data_root()

    assert root == (tmp_path / "Roaming" / "wcbt")


def test_profile_paths_resolve_within_data_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    paths = resolve_profile_paths("default", data_root=tmp_path)
    ensure_profile_directories(paths)
    assert paths.profile_root.exists()
    assert paths.manifests_root.exists()
    assert paths.archives_root.exists()
    assert paths.work_root.exists()


@pytest.mark.parametrize("bad_name", ["", " ", ".", "..", "a/b", r"a\b", "a:b", "a|b"])
def test_profile_name_rejected(bad_name: str, tmp_path: Path) -> None:
    with pytest.raises(SafetyViolationError):
        resolve_profile_paths(bad_name, data_root=tmp_path)


def test_validate_restore_target_accepts_normal_world_folder(tmp_path: Path) -> None:
    server_root = tmp_path / "server"
    server_root.mkdir()
    target = validate_restore_target(server_root, "world")
    assert target == (server_root / "world").resolve()


@pytest.mark.parametrize("bad_folder", ["", " ", ".", "..", "a/b", r"a\b", "c:world"])
def test_validate_restore_target_rejects_bad_world_folder(tmp_path: Path, bad_folder: str) -> None:
    server_root = tmp_path / "server"
    server_root.mkdir()
    with pytest.raises(SafetyViolationError):
        validate_restore_target(server_root, bad_folder)


def test_validate_restore_target_blocks_windows_system_paths() -> None:
    if os.name != "nt":
        pytest.skip("Windows-specific safety check")
    with pytest.raises(SafetyViolationError):
        validate_restore_target(Path(r"C:\Windows"), "System32")
