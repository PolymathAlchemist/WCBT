from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, cast

import pytest

from backup_engine.profile_lock import (
    ProfileLockError,
    acquire_profile_lock,
    build_profile_lock_path,
)


def _write_raw(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _load_json(path: Path) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], json.loads(path.read_text(encoding="utf-8")))


def test_acquire_profile_lock_creates_and_releases_lock(tmp_path: Path) -> None:
    lock_path = build_profile_lock_path(work_root=tmp_path)
    assert not lock_path.exists()

    with acquire_profile_lock(
        lock_path=lock_path,
        profile_name="p",
        command="backup",
        run_id="RID",
        force=False,
        break_lock=False,
    ):
        assert lock_path.exists()
        payload = _load_json(lock_path)
        assert payload["profile_name"] == "p"
        assert payload["command"] == "backup"
        assert payload["run_id"] == "RID"

    assert not lock_path.exists()


def test_existing_unreadable_lock_blocks_without_break_lock(tmp_path: Path) -> None:
    lock_path = build_profile_lock_path(work_root=tmp_path)
    _write_raw(lock_path, "not json\n")

    with pytest.raises(ProfileLockError) as excinfo:
        with acquire_profile_lock(
            lock_path=lock_path,
            profile_name="p",
            command="backup",
            run_id=None,
            force=False,
            break_lock=False,
        ):
            raise AssertionError("unreachable")

    msg = str(excinfo.value)
    assert "could not be read" in msg
    assert lock_path.exists()


def test_existing_unreadable_lock_can_be_broken_with_break_lock(tmp_path: Path) -> None:
    lock_path = build_profile_lock_path(work_root=tmp_path)
    _write_raw(lock_path, "not json\n")

    with acquire_profile_lock(
        lock_path=lock_path,
        profile_name="p",
        command="backup",
        run_id=None,
        force=False,
        break_lock=True,
    ):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_provably_stale_lock_requires_force_unless_break_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("backup_engine.profile_lock.platform.node", lambda: "HOST")
    monkeypatch.setattr("backup_engine.profile_lock.is_pid_running", lambda _pid: False)

    lock_path = build_profile_lock_path(work_root=tmp_path)
    stale_payload: dict[str, Any] = {
        "schema_version": "wcbt_profile_lock_v1",
        "profile_name": "p",
        "created_at_utc": "2026-01-01T00:00:00Z",
        "hostname": "HOST",
        "pid": 1234,
        "command": "backup",
        "run_id": None,
    }
    _write_raw(lock_path, json.dumps(stale_payload) + "\n")

    with pytest.raises(ProfileLockError) as excinfo:
        with acquire_profile_lock(
            lock_path=lock_path,
            profile_name="p",
            command="backup",
            run_id=None,
            force=False,
            break_lock=False,
        ):
            raise AssertionError("unreachable")

    assert "Re-run with --force" in str(excinfo.value)

    with acquire_profile_lock(
        lock_path=lock_path,
        profile_name="p",
        command="backup",
        run_id=None,
        force=True,
        break_lock=False,
    ):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_not_provably_stale_lock_blocks_without_break_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("backup_engine.profile_lock.platform.node", lambda: "HOST")
    monkeypatch.setattr("backup_engine.profile_lock.is_pid_running", lambda _pid: None)

    lock_path = build_profile_lock_path(work_root=tmp_path)
    payload: dict[str, Any] = {
        "schema_version": "wcbt_profile_lock_v1",
        "profile_name": "p",
        "created_at_utc": "2026-01-01T00:00:00Z",
        "hostname": "HOST",
        "pid": 1234,
        "command": "backup",
        "run_id": None,
    }
    _write_raw(lock_path, json.dumps(payload) + "\n")

    with pytest.raises(ProfileLockError) as excinfo:
        with acquire_profile_lock(
            lock_path=lock_path,
            profile_name="p",
            command="backup",
            run_id=None,
            force=True,
            break_lock=False,
        ):
            raise AssertionError("unreachable")

    assert "not provably stale" in str(excinfo.value)
    assert lock_path.exists()
