from __future__ import annotations

from pathlib import Path

import pytest

import wcbt.cli as cli_module
from backup_engine.errors import InvalidScheduleError
from backup_engine.job_binding import JobBinding
from backup_engine.scheduling.models import BackupScheduleSpec, ScheduledBackupStatus


def test_cli_schedule_create_rejects_day_with_daily(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli_module.main(
        [
            "schedule",
            "create",
            "--profile",
            "p",
            "--job-id",
            "job1",
            "--source",
            str(Path("C:/tmp/source")),
            "--daily",
            "--day",
            "MON",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 2
    assert "--day is only valid with --weekly" in out


def test_cli_schedule_create_rejects_weekly_without_day(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli_module.main(
        [
            "schedule",
            "create",
            "--profile",
            "p",
            "--job-id",
            "job1",
            "--source",
            str(Path("C:/tmp/source")),
            "--weekly",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 2
    assert "--weekly requires at least one --day" in out


def test_cli_schedule_create_prints_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _create_or_update_scheduled_backup(**kwargs: object) -> ScheduledBackupStatus:
        schedule = kwargs["schedule"]
        assert isinstance(schedule, BackupScheduleSpec)
        return ScheduledBackupStatus(
            schedule=schedule,
            current_job_binding=JobBinding(
                job_id="job1",
                job_name="My Job",
                template_id="job1",
                source_root="C:/tmp/source",
            ),
            current_template_compression="none",
            task_name="WCBT-default-job1",
            task_exists=True,
            scheduler_details={},
        )

    monkeypatch.setattr(
        cli_module, "create_or_update_scheduled_backup", _create_or_update_scheduled_backup
    )

    rc = cli_module.main(
        [
            "schedule",
            "create",
            "--profile",
            "p",
            "--job-id",
            "job1",
            "--source",
            str(Path("C:/tmp/source")),
            "--daily",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "Scheduled task : WCBT-default-job1" in out
    assert "Task exists    : yes" in out


def test_cli_schedule_query_returns_2_on_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _query_scheduled_backup(**kwargs: object) -> None:
        del kwargs
        raise InvalidScheduleError("bad schedule")

    monkeypatch.setattr(cli_module, "query_scheduled_backup", _query_scheduled_backup)

    rc = cli_module.main(["schedule", "query", "--profile", "p", "--job-id", "job1"])
    out = capsys.readouterr().out
    assert rc == 2
    assert "ERROR: bad schedule" in out


def test_cli_scheduled_backup_uses_saved_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def _load_scheduled_backup_run_request(
        **kwargs: object,
    ) -> tuple[JobBinding, str]:
        del kwargs
        return (
            JobBinding(
                job_id="job1",
                job_name="My Job",
                template_id="template-1",
                source_root="C:/tmp/source",
            ),
            "zip",
        )

    def _run_backup(**kwargs: object) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(
        cli_module, "load_scheduled_backup_run_request", _load_scheduled_backup_run_request
    )
    monkeypatch.setattr(cli_module, "run_backup", _run_backup)

    rc = cli_module.main(["scheduled-backup", "--profile", "p", "--job-id", "job1"])

    assert rc == 0
    assert seen["execute"] is True
    assert seen["compress"] is True
    assert seen["compression"] == "zip"
    assert seen["job_name"] == "My Job"
