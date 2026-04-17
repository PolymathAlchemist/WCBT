from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from backup_engine.errors import InvalidScheduleError, SchedulingBackendError
from backup_engine.job_binding import JobBinding
from backup_engine.paths_and_safety import ProfilePaths
from backup_engine.profile_store.sqlite_store import SqliteProfileStore, open_profile_store
from backup_engine.scheduling.models import BackupScheduleSpec, ScheduledTaskInfo
from backup_engine.scheduling.schtasks_backend import SchtasksBackend
from backup_engine.scheduling.service import (
    create_or_update_scheduled_backup,
    load_scheduled_backup_run_request,
    query_scheduled_backup,
    run_scheduled_backup_now,
    run_scheduled_job,
    scheduled_task_name,
    set_scheduled_backup_enabled,
)
from backup_engine.scheduling.wrapper_scripts import build_schtasks_wrapper_command


def _seed_job_state(*, data_root: Path, compression: str = "zip") -> tuple[SqliteProfileStore, str]:
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
    store.save_template_compression(job_id=job_id, name="My Job", compression=compression)
    return store, job_id


class _BackendStub(SchtasksBackend):
    """Test double that records calls instead of invoking schtasks."""

    def __init__(self) -> None:
        super().__init__(runner=lambda *args, **kwargs: None)  # type: ignore[arg-type]
        self.created: list[dict[str, object]] = []
        self.deleted: list[str] = []
        self.ran: list[str] = []
        self.enabled_changes: list[tuple[str, bool]] = []
        self.exists = True
        self.enabled = True

    def create_task(
        self,
        *,
        task_name: str,
        task_command: str,
        cadence: str,
        start_time_local: str,
        weekdays: Sequence[str],
        interval_unit: str | None = None,
        interval_value: int | None = None,
    ) -> None:
        self.created.append(
            {
                "task_name": task_name,
                "task_command": task_command,
                "cadence": cadence,
                "start_time_local": start_time_local,
                "weekdays": weekdays,
                "interval_unit": interval_unit,
                "interval_value": interval_value,
            }
        )

    def query_task(self, *, task_name: str) -> ScheduledTaskInfo:
        details = {"TaskName": task_name} if self.exists else {}
        if self.exists:
            details["Status"] = "Ready" if self.enabled else "Disabled"
        return ScheduledTaskInfo(
            task_name=task_name,
            exists=self.exists,
            details=details,
            raw_output="TaskName: something" if self.exists else None,
        )

    def delete_task(self, *, task_name: str) -> None:
        self.deleted.append(task_name)

    def run_task(self, *, task_name: str) -> None:
        self.ran.append(task_name)

    def set_task_enabled(self, *, task_name: str, enabled: bool) -> None:
        self.enabled_changes.append((task_name, enabled))
        self.enabled = enabled


def test_scheduled_task_name_is_stable() -> None:
    assert scheduled_task_name(profile_name="default", job_id="abc123") == "WCBT-default-abc123"


def test_create_schedule_persists_data_and_builds_wrapper_task_command(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path, compression="zip")
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
    )

    saved = store.load_backup_schedule(job_id)
    assert saved.start_time_local == "06:30"
    assert saved.weekdays == ("MON", "FRI")
    assert saved.compression == "zip"
    assert status.current_job_binding == JobBinding(
        job_id=job_id,
        job_name="My Job",
        template_id=status.current_job_binding.template_id,
        source_root="C:/games/world",
    )
    assert status.current_job_binding.template_id != job_id
    assert status.current_template_compression == "zip"
    assert backend.created[0]["task_name"] == f"WCBT-default-{job_id}"
    task_command = str(backend.created[0]["task_command"])
    wrapper_path = Path(status.wrapper_path)
    assert task_command == build_schtasks_wrapper_command(wrapper_path)
    assert wrapper_path.is_file()
    wrapper_text = wrapper_path.read_text(encoding="utf-8")
    assert "uv run python -m wcbt run-job" in wrapper_text
    assert "--mode execute-compress" in wrapper_text
    assert status.wrapper_exists is True
    assert status.task_exists is True
    assert status.task_enabled is True


def test_query_schedule_reports_missing_task_without_losing_persisted_data(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path, compression="none")
    wrapper_path = tmp_path / "profiles" / "default" / "scheduled_wrappers" / f"{job_id}.bat"
    wrapper_path.parent.mkdir(parents=True, exist_ok=True)
    wrapper_path.write_text("@echo off\r\n", encoding="utf-8")
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
    backend = _BackendStub()
    backend.exists = False

    status = query_scheduled_backup(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
        backend=backend,
    )

    assert status.schedule.job_id == job_id
    assert status.current_job_binding.source_root == "C:/games/world"
    assert status.current_template_compression == "none"
    assert status.wrapper_exists is True
    assert status.task_exists is False


def test_run_scheduled_backup_now_uses_derived_task_name(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path, compression="none")
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
    backend = _BackendStub()

    run_scheduled_backup_now(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
        backend=backend,
    )

    assert backend.ran == [f"WCBT-default-{job_id}"]


def test_set_scheduled_backup_enabled_updates_windows_task_state(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path, compression="none")
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
    backend = _BackendStub()

    status = set_scheduled_backup_enabled(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
        enabled=False,
        backend=backend,
    )

    assert backend.enabled_changes == [(f"WCBT-default-{job_id}", False)]
    assert status.task_enabled is False


def test_load_scheduled_backup_run_request_returns_schedule_and_job_name(tmp_path: Path) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path, compression="tar.zst")
    store.save_backup_schedule(
        BackupScheduleSpec(
            job_id=job_id,
            source_root="scheduler-owned-trigger-only",
            cadence="daily",
            start_time_local="07:00",
            weekdays=(),
            compression="tar.zst",
        )
    )

    job_binding, compression = load_scheduled_backup_run_request(
        profile_name="default",
        data_root=tmp_path,
        job_id=job_id,
    )

    assert job_binding == JobBinding(
        job_id=job_id,
        job_name="My Job",
        template_id=job_binding.template_id,
        source_root="C:/games/world",
    )
    assert compression == "tar.zst"


def test_run_scheduled_job_uses_execute_compress_mode_and_windows_note(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seen: dict[str, object] = {}

    def _load_scheduled_backup_run_request(**kwargs: object) -> tuple[JobBinding, str]:
        del kwargs
        return (
            JobBinding(
                job_id="job1",
                job_name="My Job",
                template_id="template-1",
                source_root="C:/tmp/source",
            ),
            "none",
        )

    def _run_backup(**kwargs: object) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(
        "backup_engine.scheduling.service.load_scheduled_backup_run_request",
        _load_scheduled_backup_run_request,
    )
    monkeypatch.setattr("backup_engine.backup.service.run_backup", _run_backup)
    monkeypatch.setattr(
        "backup_engine.scheduling.service.resolve_profile_paths",
        lambda **kwargs: ProfilePaths(
            data_root=tmp_path / "data",
            profile_root=tmp_path / "data" / "profiles" / "default",
            work_root=tmp_path / "data" / "profiles" / "default" / "work",
            manifests_root=tmp_path / "data" / "profiles" / "default" / "manifests",
            archives_root=tmp_path / "data" / "profiles" / "default" / "archives",
            index_root=tmp_path / "data" / "profiles" / "default" / "index",
            logs_root=tmp_path / "data" / "profiles" / "default" / "logs",
            live_snapshots_root=tmp_path / "data" / "profiles" / "default" / "live_snapshots",
        ),
    )

    run_scheduled_job(
        profile_name="default",
        data_root=None,
        job_id="job1",
        mode="execute-compress",
    )

    assert seen["execute"] is True
    assert seen["compress"] is True
    assert seen["compression"] == "none"
    assert seen["backup_origin"] == "scheduled"
    assert seen["backup_note"] == "Scheduled backup executed by Windows Task Scheduler"


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


def test_create_interval_schedule_persists_interval_fields_and_builds_backend_request(
    tmp_path: Path,
) -> None:
    store, job_id = _seed_job_state(data_root=tmp_path, compression="zip")
    backend = _BackendStub()

    status = create_or_update_scheduled_backup(
        profile_name="default",
        data_root=tmp_path,
        schedule=BackupScheduleSpec(
            job_id=job_id,
            source_root="scheduler-owned-trigger-only",
            cadence="interval",
            start_time_local="08:15",
            weekdays=(),
            compression="none",
            interval_unit="minutes",
            interval_value=10,
        ),
        backend=backend,
    )

    saved = store.load_backup_schedule(job_id)
    assert saved.cadence == "interval"
    assert saved.interval_unit == "minutes"
    assert saved.interval_value == 10
    assert saved.weekdays == ()
    assert backend.created[0]["cadence"] == "interval"
    assert backend.created[0]["interval_unit"] == "minutes"
    assert backend.created[0]["interval_value"] == 10
    assert status.schedule.interval_unit == "minutes"
    assert status.schedule.interval_value == 10


def test_create_schedule_raises_when_post_save_task_verification_fails(tmp_path: Path) -> None:
    _store, job_id = _seed_job_state(data_root=tmp_path, compression="zip")
    backend = _BackendStub()
    backend.exists = False

    with pytest.raises(SchedulingBackendError, match="Task Scheduler did not confirm"):
        create_or_update_scheduled_backup(
            profile_name="default",
            data_root=tmp_path,
            schedule=BackupScheduleSpec(
                job_id=job_id,
                source_root="scheduler-owned-trigger-only",
                cadence="daily",
                start_time_local="07:00",
                weekdays=(),
                compression="none",
            ),
            backend=backend,
        )
