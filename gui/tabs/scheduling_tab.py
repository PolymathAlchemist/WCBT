"""
Scheduling tab for WCBT GUI.

This tab provides a minimal control surface for Windows Task Scheduler-backed
backup triggers for existing jobs.
"""

from __future__ import annotations

from PySide6.QtCore import QMetaObject, QObject, Qt, QThread, Signal, Slot
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from backup_engine.errors import InvalidScheduleError
from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.scheduling.models import (
    BackupScheduleSpec,
    ScheduledBackupStatus,
    normalize_start_time_local,
)
from backup_engine.scheduling.service import (
    create_or_update_scheduled_backup,
    delete_scheduled_backup,
    query_scheduled_backup,
    run_scheduled_backup_now,
    set_scheduled_backup_enabled,
)
from gui.adapters.profile_store_adapter import ProfileStoreAdapter
from gui.settings_store import load_gui_settings


def _mono() -> QFont:
    font = QFont("Consolas")
    font.setStyleHint(QFont.Monospace)
    return font


class SchedulingWorker(QObject):
    """
    Background worker for scheduling service operations.

    Responsibilities
    ----------------
    - Call scheduling services off the UI thread.
    - Translate missing-schedule cases into a dedicated signal.
    - Emit success or failure signals back to the tab.
    """

    schedule_loaded = Signal(object)  # ScheduledBackupStatus
    schedule_saved = Signal(object)  # ScheduledBackupStatus
    schedule_deleted = Signal(str)  # job_id
    schedule_started = Signal(str)  # job_id
    schedule_state_changed = Signal(object)  # ScheduledBackupStatus
    schedule_missing = Signal(str)  # job_id
    failed = Signal(str)

    def __init__(self, *, profile_name: str, data_root) -> None:
        """
        Initialize the worker with the active profile-store context.

        Parameters
        ----------
        profile_name : str
            Active profile name used for scheduling operations.
        data_root : Path | None
            Active profile-store data root from GUI settings.
        """
        super().__init__()
        self._profile_name = profile_name
        self._data_root = data_root

    @Slot(str)
    def load_schedule(self, job_id: str) -> None:
        """
        Load the saved schedule for a job.

        Parameters
        ----------
        job_id:
            Stable job identifier.
        """
        try:
            status = query_scheduled_backup(
                profile_name=self._profile_name,
                data_root=self._data_root,
                job_id=job_id,
            )
        except UnknownJobError:
            self.schedule_missing.emit(job_id)
            return
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.schedule_loaded.emit(status)

    @Slot(object)
    def save_schedule(self, schedule_obj: object) -> None:
        """
        Save or replace a schedule for a job.

        Parameters
        ----------
        schedule_obj:
            Candidate schedule payload emitted from the GUI layer.
        """
        try:
            schedule = schedule_obj
            assert isinstance(schedule, BackupScheduleSpec)
            status = create_or_update_scheduled_backup(
                profile_name=self._profile_name,
                data_root=self._data_root,
                schedule=schedule,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.schedule_saved.emit(status)

    @Slot(str)
    def delete_schedule(self, job_id: str) -> None:
        """
        Delete the saved schedule for a job.

        Parameters
        ----------
        job_id:
            Stable job identifier.
        """
        try:
            delete_scheduled_backup(
                profile_name=self._profile_name,
                data_root=self._data_root,
                job_id=job_id,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.schedule_deleted.emit(job_id)

    @Slot(str)
    def run_schedule_now(self, job_id: str) -> None:
        """
        Start the scheduled task for a job immediately.

        Parameters
        ----------
        job_id:
            Stable job identifier.
        """
        try:
            run_scheduled_backup_now(
                profile_name=self._profile_name,
                data_root=self._data_root,
                job_id=job_id,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.schedule_started.emit(job_id)

    @Slot(str, bool)
    def set_schedule_enabled(self, job_id: str, enabled: bool) -> None:
        """
        Enable or disable the scheduled task for a job.

        Parameters
        ----------
        job_id:
            Stable job identifier.
        enabled:
            Desired task enabled state.
        """
        try:
            status = set_scheduled_backup_enabled(
                profile_name=self._profile_name,
                data_root=self._data_root,
                job_id=job_id,
                enabled=enabled,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.schedule_state_changed.emit(status)


class SchedulingTab(QWidget):
    """
    Scheduling tab for the WCBT GUI.

    Responsibilities
    ----------------
    - Allow the user to create or update a scheduled backup trigger for an existing job.
    - Show the saved trigger and current Windows task presence.
    - Delete a saved schedule or start it on demand.
    """

    def __init__(self) -> None:
        """
        Initialize the scheduling tab and background workers.
        """
        super().__init__()
        self._profile_name = "default"
        self._settings = load_gui_settings(data_root=None)
        self._has_schedule = False

        self._store = ProfileStoreAdapter(
            profile_name=self._profile_name,
            data_root=self._settings.data_root,
        )
        self._store.jobs_loaded.connect(self._on_jobs_loaded)
        self._store.error.connect(self._on_store_error)

        self._thread = QThread(self)
        self._worker = SchedulingWorker(
            profile_name=self._profile_name,
            data_root=self._settings.data_root,
        )
        self._worker.moveToThread(self._thread)
        self._worker.schedule_loaded.connect(self._on_schedule_loaded)
        self._worker.schedule_saved.connect(self._on_schedule_saved)
        self._worker.schedule_deleted.connect(self._on_schedule_deleted)
        self._worker.schedule_started.connect(self._on_schedule_started)
        self._worker.schedule_state_changed.connect(self._on_schedule_state_changed)
        self._worker.schedule_missing.connect(self._on_schedule_missing)
        self._worker.failed.connect(self._on_worker_failed)
        self._thread.start()

        outer = QVBoxLayout(self)
        outer.setContentsMargins(12, 12, 12, 12)

        schedule_box = QGroupBox("Schedule")
        schedule_layout = QVBoxLayout(schedule_box)

        job_row = QHBoxLayout()
        self.job_combo = QComboBox()
        self.job_combo.setEnabled(False)
        self.job_combo.currentIndexChanged.connect(self._on_job_changed)
        job_row.addWidget(QLabel("Job:"))
        job_row.addWidget(self.job_combo, 1)
        schedule_layout.addLayout(job_row)

        source_row = QHBoxLayout()
        self.source_edit = QLineEdit()
        self.source_edit.setReadOnly(True)
        self.source_edit.setPlaceholderText("Current job source is shown here.")
        source_row.addWidget(QLabel("Current job source:"))
        source_row.addWidget(self.source_edit, 1)
        schedule_layout.addLayout(source_row)

        cadence_row = QHBoxLayout()
        self.cadence_combo = QComboBox()
        self.cadence_combo.addItem("Daily", "daily")
        self.cadence_combo.addItem("Weekly", "weekly")
        self.cadence_combo.addItem("Interval", "interval")
        self.cadence_combo.currentIndexChanged.connect(self._sync_weekday_enabled_state)
        self.start_time_edit = QLineEdit()
        self.start_time_edit.setPlaceholderText("HH:MM")
        cadence_row.addWidget(QLabel("Cadence:"))
        cadence_row.addWidget(self.cadence_combo, 1)
        cadence_row.addSpacing(12)
        cadence_row.addWidget(QLabel("Start time:"))
        cadence_row.addWidget(self.start_time_edit, 1)
        schedule_layout.addLayout(cadence_row)

        weekday_row = QHBoxLayout()
        weekday_row.addWidget(QLabel("Weekdays:"))
        self._weekday_checks: dict[str, QCheckBox] = {}
        for day_token in ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"):
            check = QCheckBox(day_token)
            self._weekday_checks[day_token] = check
            weekday_row.addWidget(check)
        weekday_row.addStretch(1)
        schedule_layout.addLayout(weekday_row)

        interval_row = QHBoxLayout()
        interval_row.addWidget(QLabel("Repeat every:"))
        self.interval_value_spin = QSpinBox()
        self.interval_value_spin.setRange(1, 999)
        self.interval_value_spin.setValue(10)
        self.interval_unit_combo = QComboBox()
        self.interval_unit_combo.addItem("Minutes", "minutes")
        self.interval_unit_combo.addItem("Hours", "hours")
        interval_row.addWidget(self.interval_value_spin)
        interval_row.addWidget(self.interval_unit_combo)
        interval_row.addStretch(1)
        schedule_layout.addLayout(interval_row)

        compression_row = QHBoxLayout()
        self.compression_edit = QLineEdit()
        self.compression_edit.setReadOnly(True)
        self.compression_edit.setPlaceholderText("Current template compression is shown here.")
        compression_row.addWidget(QLabel("Current template compression:"))
        compression_row.addWidget(self.compression_edit, 1)
        schedule_layout.addLayout(compression_row)

        action_row = QHBoxLayout()
        self.btn_refresh = QPushButton("Refresh")
        self.btn_refresh.clicked.connect(self._refresh_current_job)
        self.btn_save = QPushButton("Save Schedule")
        self.btn_save.clicked.connect(self._save_schedule)
        self.btn_delete = QPushButton("Delete Schedule")
        self.btn_delete.clicked.connect(self._delete_schedule)
        self.btn_enable = QPushButton("Enable Task")
        self.btn_enable.clicked.connect(self._enable_schedule)
        self.btn_disable = QPushButton("Disable Task")
        self.btn_disable.clicked.connect(self._disable_schedule)
        self.btn_run_now = QPushButton("Run Now")
        self.btn_run_now.clicked.connect(self._run_schedule_now)
        self.btn_copy_task_query = QPushButton("Copy Task Query")
        self.btn_copy_task_query.clicked.connect(self._copy_task_query_command)
        action_row.addWidget(self.btn_refresh)
        action_row.addStretch(1)
        action_row.addWidget(self.btn_save)
        action_row.addWidget(self.btn_delete)
        action_row.addWidget(self.btn_enable)
        action_row.addWidget(self.btn_disable)
        action_row.addWidget(self.btn_run_now)
        action_row.addWidget(self.btn_copy_task_query)
        schedule_layout.addLayout(action_row)

        outer.addWidget(schedule_box)

        status_box = QGroupBox("Status")
        status_layout = QVBoxLayout(status_box)
        self.status_label = QLabel("Loading jobs…")
        self.status_label.setStyleSheet("padding: 6px;")
        self.summary = QPlainTextEdit()
        self.summary.setReadOnly(True)
        self.summary.setFont(_mono())
        self.summary.setPlainText("No schedule loaded.")
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.summary, 1)
        outer.addWidget(status_box, 1)

        self._set_default_form_values()
        self._sync_weekday_enabled_state()
        self._sync_action_enabled_state()

        try:
            self._store.request_list_jobs.emit()
        except Exception:
            self.shutdown()
            raise

    def _set_default_form_values(self) -> None:
        """
        Reset the form to a default schedule state.

        Notes
        -----
        This keeps the empty-state experience predictable when no schedule has
        been saved yet for the selected job.
        """
        self.source_edit.setText("")
        self.cadence_combo.setCurrentIndex(0)
        self.start_time_edit.setText("06:30")
        self.compression_edit.setText("")
        self.interval_value_spin.setValue(10)
        self.interval_unit_combo.setCurrentIndex(0)
        for check in self._weekday_checks.values():
            check.setChecked(False)

    def _selected_job_id(self) -> str | None:
        """
        Return the currently selected job identifier.

        Returns
        -------
        str | None
            Selected job identifier, or ``None`` if no job is selected.
        """
        if self.job_combo.currentIndex() < 0:
            return None
        return str(self.job_combo.currentData())

    def _selected_weekdays(self) -> tuple[str, ...]:
        """
        Return the selected weekday tokens in scheduler order.

        Returns
        -------
        tuple[str, ...]
            Selected weekday tokens.
        """
        if str(self.cadence_combo.currentData()) != "weekly":
            return ()
        return tuple(
            day_token for day_token, check in self._weekday_checks.items() if check.isChecked()
        )

    def _load_current_job_defaults(self, job_id: str) -> None:
        """
        Load current Job/Template state for a job without requiring a schedule.

        Parameters
        ----------
        job_id:
            Stable job identifier.
        """
        store = open_profile_store(
            profile_name=self._profile_name,
            data_root=self._settings.data_root,
        )
        binding = store.load_job_binding(job_id)
        compression = store.load_template_compression(job_id)
        self.source_edit.setText(binding.source_root)
        self.compression_edit.setText(compression)

    def _resolve_selected_job_defaults(self) -> tuple[str, str]:
        """
        Return authoritative Job/Template inputs for the selected job.

        Returns
        -------
        tuple[str, str]
            Current job source root and current template compression.

        Raises
        ------
        InvalidScheduleError
            If the selected job is missing required authored state.
        UnknownJobError
            If the selected job no longer exists.
        """
        job_id = self._selected_job_id()
        if job_id is None:
            raise InvalidScheduleError("Select a job first.")

        store = open_profile_store(
            profile_name=self._profile_name,
            data_root=self._settings.data_root,
        )
        binding = store.load_job_binding(job_id)
        compression = store.load_template_compression(job_id)
        source_root = binding.source_root.strip()
        if not source_root:
            raise InvalidScheduleError("Set the job source in Authoring before saving a schedule.")
        return source_root, compression

    def refresh_on_activate(self) -> None:
        """
        Refresh the available job list when the tab becomes active.

        Notes
        -----
        The Scheduling tab stays mounted while jobs can be created or renamed in
        Authoring. Refreshing on activation keeps the selector current without
        redesigning the scheduling flow.
        """
        self._store.request_list_jobs.emit()

    def _sync_weekday_enabled_state(self) -> None:
        """
        Enable or disable weekday controls based on cadence.
        """
        weekly_enabled = str(self.cadence_combo.currentData()) == "weekly"
        interval_enabled = str(self.cadence_combo.currentData()) == "interval"
        for check in self._weekday_checks.values():
            check.setEnabled(weekly_enabled)
        self.interval_value_spin.setEnabled(interval_enabled)
        self.interval_unit_combo.setEnabled(interval_enabled)

    def _sync_action_enabled_state(self) -> None:
        """
        Update action button enabled state from the current tab state.
        """
        has_job = self._selected_job_id() is not None
        self.btn_refresh.setEnabled(has_job)
        self.btn_save.setEnabled(has_job)
        self.btn_delete.setEnabled(has_job and self._has_schedule)
        self.btn_enable.setEnabled(has_job and self._has_schedule)
        self.btn_disable.setEnabled(has_job and self._has_schedule)
        self.btn_run_now.setEnabled(has_job and self._has_schedule)
        self.btn_copy_task_query.setEnabled(has_job)

    def _refresh_current_job(self) -> None:
        """
        Reload scheduling state for the selected job.
        """
        job_id = self._selected_job_id()
        if job_id is None:
            return
        try:
            self._load_current_job_defaults(job_id)
        except UnknownJobError:
            self.source_edit.setText("")
            self.compression_edit.setText("")
        self.status_label.setText("Loading schedule…")
        QMetaObject.invokeMethod(
            self._worker,
            "load_schedule",
            Qt.ConnectionType.QueuedConnection,
            job_id,
        )

    def _save_schedule(self) -> None:
        """
        Validate the form and save the schedule trigger for the selected job.
        """
        job_id = self._selected_job_id()
        if job_id is None:
            QMessageBox.information(self, "Scheduling", "Select a job first.")
            return

        try:
            source_root, compression = self._resolve_selected_job_defaults()
        except (InvalidScheduleError, UnknownJobError) as exc:
            QMessageBox.information(self, "Scheduling", str(exc))
            return
        try:
            cadence = str(self.cadence_combo.currentData())
            interval_unit = None
            interval_value = None
            if cadence == "interval":
                interval_unit = str(self.interval_unit_combo.currentData())
                interval_value = int(self.interval_value_spin.value())
        except ValueError:
            QMessageBox.warning(
                self,
                "Scheduling",
                "Invalid interval settings.",
            )
            return
        try:
            start_time_local = normalize_start_time_local(self.start_time_edit.text())
        except Exception:
            QMessageBox.warning(
                self,
                "Scheduling",
                "Start time must use 24-hour HH:MM format.",
            )
            return

        weekdays = self._selected_weekdays()
        if cadence == "weekly" and not weekdays:
            QMessageBox.warning(
                self,
                "Scheduling",
                "Select at least one weekday for a weekly schedule.",
            )
            return

        schedule = BackupScheduleSpec(
            job_id=job_id,
            source_root=source_root,
            cadence=cadence,
            start_time_local=start_time_local,
            weekdays=weekdays,
            compression=compression,
            interval_unit=interval_unit,
            interval_value=interval_value,
        )

        self.status_label.setText("Saving schedule…")
        QMetaObject.invokeMethod(
            self._worker,
            "save_schedule",
            Qt.ConnectionType.QueuedConnection,
            schedule,
        )

    def _delete_schedule(self) -> None:
        """
        Delete the saved schedule for the selected job.
        """
        job_id = self._selected_job_id()
        if job_id is None or not self._has_schedule:
            return

        result = QMessageBox.question(
            self,
            "Delete schedule",
            "Delete the saved schedule for this job?",
        )
        if result != QMessageBox.Yes:
            return

        self.status_label.setText("Deleting schedule…")
        QMetaObject.invokeMethod(
            self._worker,
            "delete_schedule",
            Qt.ConnectionType.QueuedConnection,
            job_id,
        )

    def _run_schedule_now(self) -> None:
        """
        Start the selected job's scheduled task immediately.
        """
        job_id = self._selected_job_id()
        if job_id is None or not self._has_schedule:
            return

        self.status_label.setText("Starting scheduled task…")
        QMetaObject.invokeMethod(
            self._worker,
            "run_schedule_now",
            Qt.ConnectionType.QueuedConnection,
            job_id,
        )

    def _enable_schedule(self) -> None:
        """
        Enable the selected job's scheduled task.
        """
        self._set_schedule_enabled(True)

    def _disable_schedule(self) -> None:
        """
        Disable the selected job's scheduled task.
        """
        self._set_schedule_enabled(False)

    def _set_schedule_enabled(self, enabled: bool) -> None:
        """
        Update the selected job's scheduled task state.

        Parameters
        ----------
        enabled:
            Desired enabled state.
        """
        job_id = self._selected_job_id()
        if job_id is None or not self._has_schedule:
            return

        self.status_label.setText("Updating task state…")
        QMetaObject.invokeMethod(
            self._worker,
            "set_schedule_enabled",
            Qt.ConnectionType.QueuedConnection,
            job_id,
            enabled,
        )

    def _task_query_command(self, job_id: str) -> str:
        """
        Build the exact ``schtasks`` query command for one selected job.

        Parameters
        ----------
        job_id:
            Stable WCBT job identifier.

        Returns
        -------
        str
            Windows command-line query for the backing scheduled task.
        """
        task_name = f"WCBT-{self._profile_name}-{job_id}"
        return f'schtasks /query /tn "{task_name}" /fo LIST /v'

    def _copy_task_query_command(self) -> None:
        """
        Copy the exact Task Scheduler query command for the selected job.
        """
        job_id = self._selected_job_id()
        if job_id is None:
            QMessageBox.information(self, "Scheduling", "Select a job first.")
            return

        QApplication.clipboard().setText(self._task_query_command(job_id))
        self.status_label.setText("Task query copied.")

    def _apply_schedule_to_form(self, status: ScheduledBackupStatus) -> None:
        """
        Load a saved schedule into the form widgets.

        Parameters
        ----------
        status:
            Combined persisted trigger, task state, current authoritative Job
            binding, and current Template compression.
        """
        self.source_edit.setText(status.current_job_binding.source_root)
        if status.current_template_compression is not None:
            self.compression_edit.setText(status.current_template_compression)
        else:
            self.compression_edit.setText("")
        self.start_time_edit.setText(status.schedule.start_time_local)
        self._select_combo_by_data(self.cadence_combo, status.schedule.cadence)
        self.interval_value_spin.setValue(status.schedule.interval_value or 10)
        if status.schedule.interval_unit is not None:
            self._select_combo_by_data(self.interval_unit_combo, status.schedule.interval_unit)

        selected_days = set(status.schedule.weekdays)
        for day_token, check in self._weekday_checks.items():
            check.setChecked(day_token in selected_days)
        self._sync_weekday_enabled_state()

    @staticmethod
    def _select_combo_by_data(combo: QComboBox, value: str) -> None:
        """
        Select the first combo row whose item data matches ``value``.

        Parameters
        ----------
        combo:
            Combo box to update.
        value:
            Desired item data value.
        """
        for index in range(combo.count()):
            if str(combo.itemData(index)) == value:
                combo.setCurrentIndex(index)
                return

    def _render_schedule_summary(self, status: ScheduledBackupStatus) -> str:
        """
        Render the saved trigger and task state for display.

        Parameters
        ----------
        status:
            Combined persisted trigger, task state, current authoritative Job
            binding, and current Template compression.

        Returns
        -------
        str
            Human-readable summary text.
        """
        lines: list[str] = []
        lines.append("SCHEDULE TRIGGER")
        lines.append(f"  task_name: {status.task_name}")
        lines.append(f"  task_query: {self._task_query_command(status.schedule.job_id)}")
        lines.append(f"  cadence: {status.schedule.cadence}")
        lines.append(f"  start_time_local: {status.schedule.start_time_local}")
        if status.schedule.cadence == "interval":
            lines.append(
                f"  interval: {status.schedule.interval_value} {status.schedule.interval_unit}"
            )
        lines.append(
            f"  weekdays: {','.join(status.schedule.weekdays) if status.schedule.weekdays else '-'}"
        )
        lines.append(f"  wrapper_path: {status.wrapper_path}")
        lines.append(f"  wrapper_exists: {str(status.wrapper_exists).lower()}")
        lines.append(f"  task_exists: {str(status.task_exists).lower()}")
        if status.task_enabled is not None:
            lines.append(f"  task_enabled: {str(status.task_enabled).lower()}")
        lines.append("")
        lines.append("CURRENT JOB BINDING")
        lines.append(f"  job_id: {status.current_job_binding.job_id}")
        lines.append(f"  job_name: {status.current_job_binding.job_name}")
        lines.append(f"  template_id: {status.current_job_binding.template_id}")
        lines.append(f"  source_root: {status.current_job_binding.source_root}")
        if status.current_template_compression is not None:
            lines.append("")
            lines.append("CURRENT TEMPLATE POLICY VIEW")
            lines.append(f"  compression: {status.current_template_compression}")
        if status.scheduler_details:
            lines.append("")
            lines.append("TASK SCHEDULER")
            for key in ("Status", "Last Run Time", "Last Result", "Next Run Time", "Task To Run"):
                value = status.scheduler_details.get(key)
                if value:
                    lines.append(f"  {key}: {value}")
        return "\n".join(lines)

    def _on_job_changed(self) -> None:
        """
        Reload scheduling state for the newly selected job.
        """
        self._has_schedule = False
        self._sync_action_enabled_state()
        if self._selected_job_id() is None:
            self.status_label.setText("No job selected.")
            self.summary.setPlainText("No schedule loaded.")
            return
        self._refresh_current_job()

    def _on_jobs_loaded(self, jobs_obj: object) -> None:
        """
        Populate the job selector from the profile store.

        Parameters
        ----------
        jobs_obj:
            Job list payload emitted from the profile store adapter.
        """
        try:
            jobs = list(jobs_obj)
        except Exception:
            QMessageBox.critical(self, "Profile Store Error", "Invalid job list from store.")
            return

        self.job_combo.blockSignals(True)
        try:
            self.job_combo.clear()
            for job_summary in jobs:
                job_id = str(getattr(job_summary, "job_id"))
                name = str(getattr(job_summary, "name"))
                self.job_combo.addItem(name, job_id)
        finally:
            self.job_combo.blockSignals(False)

        if self.job_combo.count() == 0:
            self.job_combo.setEnabled(False)
            self.status_label.setText("No jobs yet. Create one in Authoring.")
            self.summary.setPlainText("No schedule loaded.")
            self._sync_action_enabled_state()
            return

        self.job_combo.setEnabled(True)
        self._sync_action_enabled_state()
        self._on_job_changed()

    def _on_store_error(self, job_id: str, message: str) -> None:
        """
        Show profile store errors to the user.

        Parameters
        ----------
        job_id:
            Job identifier supplied by the adapter.
        message:
            Error message to display.
        """
        _ = job_id
        self.status_label.setText("Error")
        QMessageBox.critical(self, "Profile Store Error", message)

    def _on_schedule_loaded(self, status_obj: object) -> None:
        """
        Apply a loaded schedule to the UI.

        Parameters
        ----------
        status_obj:
            Loaded schedule payload from the worker.
        """
        status = status_obj
        assert isinstance(status, ScheduledBackupStatus)
        if status.schedule.job_id != self._selected_job_id():
            return

        self._has_schedule = True
        self._apply_schedule_to_form(status)
        self.summary.setPlainText(self._render_schedule_summary(status))
        self.status_label.setText("Schedule loaded.")
        self._sync_action_enabled_state()

    def _on_schedule_saved(self, status_obj: object) -> None:
        """
        Apply the saved schedule result to the UI.

        Parameters
        ----------
        status_obj:
            Saved schedule payload from the worker.
        """
        self._on_schedule_loaded(status_obj)
        self.status_label.setText("Schedule saved.")

    def _on_schedule_deleted(self, job_id: str) -> None:
        """
        Clear the UI after a schedule is deleted.

        Parameters
        ----------
        job_id:
            Deleted job identifier.
        """
        if job_id != self._selected_job_id():
            return
        self._has_schedule = False
        self._set_default_form_values()
        self._sync_weekday_enabled_state()
        self.summary.setPlainText("No schedule saved for this job.")
        self.status_label.setText("Schedule deleted.")
        self._sync_action_enabled_state()

    def _on_schedule_started(self, job_id: str) -> None:
        """
        Show confirmation after a scheduled task starts.

        Parameters
        ----------
        job_id:
            Job identifier whose task was started.
        """
        if job_id != self._selected_job_id():
            return
        self.status_label.setText("Scheduled task started.")
        QMessageBox.information(self, "Scheduling", "Scheduled task started.")

    def _on_schedule_state_changed(self, status_obj: object) -> None:
        """
        Refresh the UI after the task enabled state changes.

        Parameters
        ----------
        status_obj:
            Updated schedule payload from the worker.
        """
        self._on_schedule_loaded(status_obj)
        status = status_obj
        assert isinstance(status, ScheduledBackupStatus)
        if status.task_enabled is True:
            self.status_label.setText("Scheduled task enabled.")
        elif status.task_enabled is False:
            self.status_label.setText("Scheduled task disabled.")
        else:
            self.status_label.setText("Scheduled task state updated.")

    def _on_schedule_missing(self, job_id: str) -> None:
        """
        Show the empty state when a job has no saved schedule.

        Parameters
        ----------
        job_id:
            Job identifier whose schedule was requested.
        """
        if job_id != self._selected_job_id():
            return
        self._has_schedule = False
        self._set_default_form_values()
        self._load_current_job_defaults(job_id)
        self._sync_weekday_enabled_state()
        self.summary.setPlainText(
            "No schedule saved for this job.\n"
            "\n"
            "Current job source and template compression were resolved from Authoring."
        )
        self.status_label.setText("Ready to save schedule.")
        self._sync_action_enabled_state()

    def _on_worker_failed(self, message: str) -> None:
        """
        Show worker errors to the user.

        Parameters
        ----------
        message:
            Error message from the worker.
        """
        self.status_label.setText("Error")
        QMessageBox.critical(self, "Scheduling", message)

    def shutdown(self) -> None:
        """
        Shut down worker threads and adapters owned by the tab.

        Notes
        -----
        This method is intended to be safe to call multiple times.
        """
        self._thread.quit()
        self._thread.wait(2000)
        self._store.shutdown()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """
        Ensure worker shutdown when the widget closes.

        Parameters
        ----------
        event:
            Qt close event.
        """
        self.shutdown()
        super().closeEvent(event)
