from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from backup_engine.errors import InvalidScheduleError
from backup_engine.profile_store.api import JobBackupDefaults
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.scheduling.models import BackupScheduleSpec, ScheduledTaskInfo
from backup_engine.scheduling.schtasks_backend import SchtasksBackend
from backup_engine.scheduling.service import (
    create_or_update_scheduled_backup,
    load_scheduled_backup_run_request,
    query_scheduled_backup,
    run_scheduled_backup_now,
    scheduled_task_name,
)


class _BackendStub(SchtasksBackend):
    """Test double that records calls instead of invoking schtasks."""

    def __init__(self) -> None:
        super().__init__(runner=lambda *args, **kwargs: None)  # type: ignore[arg-type]
        self.created: list[dict[str, object]] = []
        self.deleted: list[str] = []
        self.ran: list[str] = []
        self.exists = True

    def create_task(
        self,
        *,
        task_name: str,
        task_command: str,
        cadence: str,
        start_time_local: str,
        weekdays: Sequence[str],
    ) -> None:
        self.created.append(
            {
                "task_name": task_name,
                "task_command": task_command,
                "cadence": cadence,
                "start_time_local": start_time_local,
                "weekdays": weekdays,
            }
        )

    def query_task(self, *, task_name: str) -> ScheduledTaskInfo:
        return ScheduledTaskInfo(
            task_name=task_name,
            exists=self.exists,
            details={"TaskName": task_name} if self.exists else {},
            raw_output="TaskName: something" if self.exists else None,
        )

    def delete_task(self, *, task_name: str) -> None:
        self.deleted.append(task_name)

    def run_task(self, *, task_name: str) -> None:
        self.ran.append(task_name)


def test_scheduled_task_name_is_stable() -> None:
    assert scheduled_task_name(profile_name="default", job_id="abc123") == "WCBT-default-abc123"


def test_create_schedule_persists_data_and_builds_task_command(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")
    backend = _BackendStub()

    status = create_or_update_scheduled_backup(
        profile_name="default",
        data_root=tmp_path,
        schedule=BackupScheduleSpec(
            job_id=job_id,
            source_root="C:/games/world",
            cadence="weekly",
            start_time_local="6:30",
            weekdays=("fri", "mon"),
            compression="zip",
        ),
        backend=backend,
        python_executable="C:\\Python\\python.exe",
    )

    saved = store.load_backup_schedule(job_id)
    assert saved.start_time_local == "06:30"
    assert saved.weekdays == ("MON", "FRI")
    assert saved.compression == "zip"
    assert backend.created[0]["task_name"] == f"WCBT-default-{job_id}"
    assert "--job-id" in str(backend.created[0]["task_command"])
    assert "scheduled-backup" in str(backend.created[0]["task_command"])
    assert status.task_exists is True


def test_query_schedule_reports_missing_task_without_losing_persisted_data(tmp_path: Path) -> None:
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
    backend = _BackendStub()
    backend.exists = False

    status = query_scheduled_backup(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
        backend=backend,
    )

    assert status.schedule.job_id == job_id
    assert status.task_exists is False


def test_run_scheduled_backup_now_uses_derived_task_name(tmp_path: Path) -> None:
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
    backend = _BackendStub()

    run_scheduled_backup_now(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
        backend=backend,
    )

    assert backend.ran == [f"WCBT-default-{job_id}"]


def test_load_scheduled_backup_run_request_returns_schedule_and_job_name(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")
    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="C:/games/world",
            cadence="daily",
            start_time_local="07:00",
            weekdays=(),
            compression="tar.zst",
        )
    )

    defaults, job_name = load_scheduled_backup_run_request(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
    )

    assert defaults == JobBackupDefaults(source_root="C:/games/world", compression="tar.zst")
    assert job_name == "My Job"


def test_create_schedule_rejects_weekly_without_days(tmp_path: Path) -> None:
    store = open_profile_store(profile_name="default", data_root=tmp_path)
    job_id = store.create_job("My Job")

    with pytest.raises(InvalidScheduleError):
        create_or_update_scheduled_backup(
            profile_name="default",
            data_root=tmp_path,
            schedule=BackupScheduleSpec(
                job_id=job_id,
                source_root="C:/games/world",
                cadence="weekly",
                start_time_local="07:00",
                weekdays=(),
                compression="none",
            ),
            backend=_BackendStub(),
        )
