from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import SqliteProfileStore, open_profile_store
from backup_engine.scheduling.models import BackupScheduleSpec


def _seed_job_state(*, data_root: Path) -> tuple[SqliteProfileStore, str]:
    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("My Job")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=job_id,
            job_name="My Job",
            template_id=binding.template_id,
            source_root="C:/games/world",
        )
    )
    store.save_template_compression(job_id=job_id, name="My Job", compression="zip")
    return store, job_id


def test_profile_store_backup_schedule_roundtrip(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path)

    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="scheduler-owned-trigger-only",
            cadence="weekly",
            start_time_local="06:45",
            weekdays=("MON", "WED"),
            compression="none",
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


def test_profile_store_interval_schedule_roundtrip(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path)

    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="scheduler-owned-trigger-only",
            cadence="interval",
            start_time_local="08:15",
            weekdays=(),
            compression="none",
            interval_unit="minutes",
            interval_value=10,
        )
    )

    loaded = store.load_backup_schedule(job_id)

    assert loaded.job_id == job_id
    assert loaded.cadence == "interval"
    assert loaded.start_time_local == "08:15"
    assert loaded.weekdays == ()
    assert loaded.interval_unit == "minutes"
    assert loaded.interval_value == 10
    assert loaded.source_root == "C:/games/world"
    assert loaded.compression == "zip"


def test_profile_store_persists_trigger_without_writing_legacy_schedule_mirror(
    tmp_path: Path,
) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path)

    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="scheduler-owned-trigger-only",
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
    assert "interval_unit" in schedule_columns
    assert "interval_value" in schedule_columns
    assert legacy_table_row is None


def test_profile_store_delete_backup_schedule_is_idempotent(tmp_path: Path) -> None:
    _store, job_id = _seed_job_state(data_root=tmp_path)

    _store.delete_backup_schedule(job_id)
    _store.delete_backup_schedule(job_id)


def test_profile_store_raises_for_missing_schedule(tmp_path: Path) -> None:
    _store, job_id = _seed_job_state(data_root=tmp_path)

    with pytest.raises(UnknownJobError):
        _store.load_backup_schedule(job_id)
