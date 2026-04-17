from __future__ import annotations

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
            "--weekly",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 2
    assert "--weekly requires at least one --day" in out


def test_cli_schedule_create_rejects_interval_without_unit_and_value(
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
            "--interval",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 2
    assert "--interval requires --interval-unit and positive --interval-value" in out


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
            wrapper_path="C:/Users/test/AppData/Local/wcbt/profiles/default/scheduled_wrappers/job1.bat",
            wrapper_exists=True,
            task_exists=True,
            task_enabled=True,
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
            "--daily",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "Scheduled task : WCBT-default-job1" in out
    assert "Wrapper        :" in out
    assert "Task exists    : yes" in out
    assert "Task enabled   : yes" in out


def test_cli_schedule_create_interval_prints_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _create_or_update_scheduled_backup(**kwargs: object) -> ScheduledBackupStatus:
        schedule = kwargs["schedule"]
        assert isinstance(schedule, BackupScheduleSpec)
        assert schedule.cadence == "interval"
        assert schedule.interval_unit == "minutes"
        assert schedule.interval_value == 10
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
            wrapper_path="C:/Users/test/AppData/Local/wcbt/profiles/default/scheduled_wrappers/job1.bat",
            wrapper_exists=True,
            task_exists=True,
            task_enabled=True,
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
            "--interval",
            "--interval-unit",
            "minutes",
            "--interval-value",
            "10",
            "--start-time",
            "06:30",
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "Cadence        : interval" in out
    assert "Interval       : 10 minutes" in out


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


def test_cli_run_job_uses_stable_scheduled_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def _run_scheduled_job(**kwargs: object) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(cli_module, "run_scheduled_job", _run_scheduled_job)

    rc = cli_module.main(
        ["run-job", "--profile", "p", "--job-id", "job1", "--mode", "execute-compress"]
    )

    assert rc == 0
    assert seen["profile_name"] == "p"
    assert seen["job_id"] == "job1"
    assert seen["mode"] == "execute-compress"


def test_cli_scheduled_backup_alias_uses_execute_compress(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def _run_scheduled_job(**kwargs: object) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(cli_module, "run_scheduled_job", _run_scheduled_job)

    rc = cli_module.main(["scheduled-backup", "--profile", "p", "--job-id", "job1"])

    assert rc == 0
    assert seen["mode"] == "execute-compress"
