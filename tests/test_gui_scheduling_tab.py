from __future__ import annotations

from pathlib import Path
from typing import cast

from _pytest.monkeypatch import MonkeyPatch
from PySide6.QtCore import QMetaObject, QObject, Signal
from PySide6.QtWidgets import QApplication, QMessageBox

from backup_engine.job_binding import JobBinding
from backup_engine.profile_store.api import JobSummary
from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.scheduling.models import BackupScheduleSpec, ScheduledBackupStatus
from gui.settings_store import GuiSettings
from gui.tabs.scheduling_tab import SchedulingTab


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return cast(QApplication, app)


class _CapturingProfileStoreAdapter(QObject):
    request_list_jobs = Signal()

    jobs_loaded = Signal(object)
    error = Signal(str, str)

    last_data_root: Path | None = None
    refresh_count = 0

    def __init__(self, profile_name: str, data_root: Path | None = None) -> None:
        super().__init__()
        _ = profile_name
        type(self).last_data_root = data_root
        self.request_list_jobs.connect(self._record_refresh)

    @classmethod
    def _record_refresh(cls) -> None:
        cls.refresh_count += 1

    def shutdown(self) -> None:
        pass


def test_scheduling_tab_uses_gui_settings_data_root_for_profile_store(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    _CapturingProfileStoreAdapter.last_data_root = None
    _CapturingProfileStoreAdapter.refresh_count = 0

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )

    tab = SchedulingTab()
    try:
        assert _CapturingProfileStoreAdapter.last_data_root == data_root
        assert _CapturingProfileStoreAdapter.refresh_count == 1
    finally:
        tab.shutdown()


def test_scheduling_tab_populates_jobs_and_loads_job_defaults_from_active_data_root(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    def _invoke_immediately(
        target: object,
        member: str,
        *_args: object,
        **_kwargs: object,
    ) -> bool:
        assert member == "load_schedule"
        getattr(target, member)(_args[1])
        return True

    monkeypatch.setattr(QMetaObject, "invokeMethod", _invoke_immediately)
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.query_scheduled_backup",
        lambda *, profile_name, data_root, job_id: (_ for _ in ()).throw(UnknownJobError(job_id)),
    )

    tab = SchedulingTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        assert tab.job_combo.isEnabled()
        assert tab.job_combo.count() == 1
        assert str(tab.job_combo.currentData()) == job_id
        assert tab.source_edit.text() == str(source_root)
        assert tab.compression_edit.text() == "zip"
        assert tab.status_label.text() == "Ready to save schedule."
        assert tab.btn_refresh.isEnabled()
        assert tab.btn_save.isEnabled()
    finally:
        tab.shutdown()


def test_scheduling_tab_populates_job_defaults_immediately_on_job_selection(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMetaObject, "invokeMethod", lambda *args, **kwargs: True)

    tab = SchedulingTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        assert tab.source_edit.text() == str(source_root)
        assert tab.compression_edit.text() == "zip"
        assert tab.status_label.text() == "Loading schedule…"
    finally:
        tab.shutdown()


def test_scheduling_tab_save_uses_authoritative_job_defaults_not_read_only_widget_text(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    captured_schedules: list[BackupScheduleSpec] = []

    def _create_or_update_scheduled_backup(**kwargs: object) -> ScheduledBackupStatus:
        schedule = kwargs["schedule"]
        assert isinstance(schedule, BackupScheduleSpec)
        captured_schedules.append(schedule)
        return ScheduledBackupStatus(
            schedule=schedule,
            current_job_binding=store.load_job_binding(job_id),
            current_template_compression=store.load_template_compression(job_id),
            task_name=f"WCBT-default-{job_id}",
            wrapper_path=str(tmp_path / f"{job_id}.bat"),
            wrapper_exists=True,
            task_exists=True,
            task_enabled=True,
            scheduler_details={},
        )

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.create_or_update_scheduled_backup",
        _create_or_update_scheduled_backup,
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    def _invoke_immediately(
        target: object,
        member: str,
        *_args: object,
        **_kwargs: object,
    ) -> bool:
        if member == "load_schedule":
            getattr(target, member)(_args[1])
            return True
        if member == "save_schedule":
            getattr(target, member)(_args[1])
            return True
        raise AssertionError(member)

    monkeypatch.setattr(QMetaObject, "invokeMethod", _invoke_immediately)
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.query_scheduled_backup",
        lambda *, profile_name, data_root, job_id: (_ for _ in ()).throw(UnknownJobError(job_id)),
    )

    tab = SchedulingTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        tab.source_edit.setText("")
        tab.compression_edit.setText("")

        tab._save_schedule()  # noqa: SLF001

        assert len(captured_schedules) == 1
        assert captured_schedules[0].source_root == str(source_root)
        assert captured_schedules[0].compression == "zip"
    finally:
        tab.shutdown()


def test_scheduling_tab_interval_save_captures_interval_fields(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    captured_schedules: list[BackupScheduleSpec] = []

    def _create_or_update_scheduled_backup(**kwargs: object) -> ScheduledBackupStatus:
        schedule = kwargs["schedule"]
        assert isinstance(schedule, BackupScheduleSpec)
        captured_schedules.append(schedule)
        return ScheduledBackupStatus(
            schedule=schedule,
            current_job_binding=store.load_job_binding(job_id),
            current_template_compression=store.load_template_compression(job_id),
            task_name=f"WCBT-default-{job_id}",
            wrapper_path=str(tmp_path / f"{job_id}.bat"),
            wrapper_exists=True,
            task_exists=True,
            task_enabled=True,
            scheduler_details={},
        )

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.create_or_update_scheduled_backup",
        _create_or_update_scheduled_backup,
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)

    def _invoke_immediately(
        target: object,
        member: str,
        *_args: object,
        **_kwargs: object,
    ) -> bool:
        if member == "load_schedule":
            getattr(target, member)(_args[1])
            return True
        if member == "save_schedule":
            getattr(target, member)(_args[1])
            return True
        raise AssertionError(member)

    monkeypatch.setattr(QMetaObject, "invokeMethod", _invoke_immediately)
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.query_scheduled_backup",
        lambda *, profile_name, data_root, job_id: (_ for _ in ()).throw(UnknownJobError(job_id)),
    )

    tab = SchedulingTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001
        for index in range(tab.cadence_combo.count()):
            if str(tab.cadence_combo.itemData(index)) == "interval":
                tab.cadence_combo.setCurrentIndex(index)
                break
        tab.interval_value_spin.setValue(10)
        for index in range(tab.interval_unit_combo.count()):
            if str(tab.interval_unit_combo.itemData(index)) == "minutes":
                tab.interval_unit_combo.setCurrentIndex(index)
                break

        tab._save_schedule()  # noqa: SLF001

        assert len(captured_schedules) == 1
        assert captured_schedules[0].cadence == "interval"
        assert captured_schedules[0].interval_unit == "minutes"
        assert captured_schedules[0].interval_value == 10
    finally:
        tab.shutdown()


def test_scheduling_tab_save_surfaces_backend_verification_failure(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    reported_errors: list[str] = []

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.create_or_update_scheduled_backup",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("Task Scheduler did not confirm the scheduled task after save.")
        ),
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        QMessageBox,
        "critical",
        lambda *args: reported_errors.append(str(args[2])),
    )

    def _invoke_immediately(
        target: object,
        member: str,
        *_args: object,
        **_kwargs: object,
    ) -> bool:
        if member == "load_schedule":
            getattr(target, member)(_args[1])
            return True
        if member == "save_schedule":
            getattr(target, member)(_args[1])
            return True
        raise AssertionError(member)

    monkeypatch.setattr(QMetaObject, "invokeMethod", _invoke_immediately)
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.query_scheduled_backup",
        lambda *, profile_name, data_root, job_id: (_ for _ in ()).throw(UnknownJobError(job_id)),
    )

    tab = SchedulingTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        tab._save_schedule()  # noqa: SLF001

        assert tab.status_label.text() == "Error"
        assert reported_errors == ["Task Scheduler did not confirm the scheduled task after save."]
    finally:
        tab.shutdown()


def test_scheduling_tab_copy_task_query_copies_exact_schtasks_command(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    _app()

    data_root = tmp_path / "data_root"
    source_root = tmp_path / "testing"
    source_root.mkdir()

    settings = GuiSettings(
        data_root=data_root,
        archives_root=None,
        default_compression="none",
        default_run_mode="plan",
    )

    store = open_profile_store(profile_name="default", data_root=data_root)
    job_id = store.create_job("Minecraft")
    binding = store.load_job_binding(job_id)
    store.save_job_binding(
        JobBinding(
            job_id=binding.job_id,
            job_name="Minecraft",
            template_id=binding.template_id,
            source_root=str(source_root),
        )
    )
    store.save_template_compression(job_id=job_id, name="Minecraft", compression="zip")

    clipboard_text: dict[str, str] = {"value": ""}

    class _Clipboard:
        def setText(self, text: str) -> None:
            clipboard_text["value"] = text

    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.load_gui_settings",
        lambda *, data_root: settings,
    )
    monkeypatch.setattr(
        "gui.tabs.scheduling_tab.ProfileStoreAdapter",
        _CapturingProfileStoreAdapter,
    )
    monkeypatch.setattr(QMessageBox, "critical", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMetaObject, "invokeMethod", lambda *args, **kwargs: True)
    monkeypatch.setattr(QApplication, "clipboard", lambda: _Clipboard())

    tab = SchedulingTab()
    try:
        tab._on_jobs_loaded([JobSummary(job_id=job_id, name="Minecraft")])  # noqa: SLF001

        tab._copy_task_query_command()  # noqa: SLF001

        assert clipboard_text["value"] == (
            f'schtasks /query /tn "WCBT-default-{job_id}" /fo LIST /v'
        )
        assert tab.status_label.text() == "Task query copied."
    finally:
        tab.shutdown()


def test_scheduling_tab_summary_includes_task_query_command() -> None:
    _app()

    tab = SchedulingTab()
    try:
        status = ScheduledBackupStatus(
            schedule=BackupScheduleSpec(
                job_id="job1",
                source_root="C:/tmp/source",
                cadence="daily",
                start_time_local="06:30",
                weekdays=(),
                compression="zip",
            ),
            current_job_binding=JobBinding(
                job_id="job1",
                job_name="My Job",
                template_id="template-1",
                source_root="C:/tmp/source",
            ),
            current_template_compression="zip",
            task_name="WCBT-default-job1",
            wrapper_path="C:/tmp/job1.bat",
            wrapper_exists=True,
            task_exists=True,
            task_enabled=True,
            scheduler_details={},
        )

        summary = tab._render_schedule_summary(status)  # noqa: SLF001

        assert "task_name: WCBT-default-job1" in summary
        assert 'task_query: schtasks /query /tn "WCBT-default-job1" /fo LIST /v' in summary
        assert "wrapper_path: C:/tmp/job1.bat" in summary
        assert "wrapper_exists: true" in summary
        assert "task_exists: true" in summary
    finally:
        tab.shutdown()
