from __future__ import annotations

import sqlite3
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
    assert store.load_job_binding(job_id).source_root == "C:/games/world"


def test_profile_store_persists_trigger_without_writing_legacy_schedule_mirror(
    tmp_path: Path,
) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")

    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="C:/games/world",
            cadence="daily",
            start_time_local="07:00",
            weekdays=(),
            compression="none",
        )
    )

    database_path = tmp_path / "profiles" / "default" / "index" / "profiles.sqlite"
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        schedule_columns = {
            str(column["name"]) for column in connection.execute("PRAGMA table_info(job_schedules)")
        }
        legacy_table_row = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'scheduled_backup_legacy_inputs'"
        ).fetchone()
    finally:
        connection.close()

    assert "source_root" not in schedule_columns
    assert "compression" not in schedule_columns
    assert legacy_table_row is None


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
