from __future__ import annotations

from pathlib import Path

import pytest

from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.scheduling.models import BackupScheduleSpec


def test_profile_store_backup_schedule_roundtrip(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")

    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="C:/games/world",
            cadence="weekly",
            start_time_local="06:45",
            weekdays=("MON", "WED"),
            compression="zip",
        )
    )

    loaded = store.load_backup_schedule(job_id)

    assert loaded.job_id == job_id
    assert loaded.source_root == "C:/games/world"
    assert loaded.cadence == "weekly"
    assert loaded.start_time_local == "06:45"
    assert loaded.weekdays == ("MON", "WED")
    assert loaded.compression == "zip"


def test_profile_store_delete_backup_schedule_is_idempotent(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")

    store.delete_backup_schedule(job_id)
    store.delete_backup_schedule(job_id)


def test_profile_store_raises_for_missing_schedule(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")

    with pytest.raises(UnknownJobError):
        store.load_backup_schedule(job_id)
