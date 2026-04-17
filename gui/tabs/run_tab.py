"""
Run tab (engine-backed).

Purpose
-------
- Allow the user to execute backup runs for a selected job.
- Dispatch backup planning/execution off the UI thread.
- Present summary output and artifact locations to the user.

Notes
-----
- This tab does not invoke the CLI.
- Backup execution is delegated directly to the engine service.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from PySide6.QtCore import (
    QMetaObject,
    QObject,
    Qt,
    QThread,
    QUrl,
    Signal,
    Slot,
)
from PySide6.QtGui import (
    QDesktopServices,
    QFont,
)
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from backup_engine.backup.service import BackupRunResult, run_backup
from backup_engine.job_binding import JobBinding
from backup_engine.oz0_paths import resolve_oz0_artifact_root
from backup_engine.profile_store.errors import UnknownJobError
from backup_engine.profile_store.sqlite_store import open_profile_store
from gui.adapters.profile_store_adapter import ProfileStoreAdapter
from gui.settings_store import GuiSettings, load_gui_settings, save_gui_settings


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class _TemplateChoice(NamedTuple):
    label: str
    template_id: str | None


class JobBindingDialog(QDialog):
    """Minimal dialog for creating or editing a Job binding."""

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        title: str,
        initial_name: str,
        initial_source_root: str,
        template_choices: list[_TemplateChoice],
        initial_template_id: str | None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(520, 180)

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.name_edit = QLineEdit(initial_name)
        form.addRow("Job name:", self.name_edit)

        self.template_combo = QComboBox()
        for choice in template_choices:
            self.template_combo.addItem(choice.label, choice.template_id)
        if initial_template_id is not None:
            for index in range(self.template_combo.count()):
                if str(self.template_combo.itemData(index) or "") == initial_template_id:
                    self.template_combo.setCurrentIndex(index)
                    break
        form.addRow("Template:", self.template_combo)

        source_row = QHBoxLayout()
        self.source_edit = QLineEdit(initial_source_root)
        self.source_edit.setPlaceholderText("Choose source folder for this job…")
        browse_button = QPushButton("Browse…")
        browse_button.clicked.connect(self._browse_source)
        source_row.addWidget(self.source_edit, 1)
        source_row.addWidget(browse_button)
        form.addRow("Source folder:", source_row)

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def selected_name(self) -> str:
        return self.name_edit.text().strip()

    def selected_template_id(self) -> str | None:
        value = self.template_combo.currentData()
        return str(value) if value is not None else None

    def selected_source_root(self) -> str:
        return self.source_edit.text().strip()

    def _browse_source(self) -> None:
        start_dir = self.source_edit.text().strip() or str(Path.home())
        directory = QFileDialog.getExistingDirectory(self, "Select source folder", start_dir)
        if directory:
            self.source_edit.setText(directory)


class BackupWorker(QObject):
    """
    Background worker that executes backup runs off the UI thread.

    Responsibilities
    ----------------
    - Invoke the engine backup service with the configured job and source.
    - Emit completion or failure signals back to the GUI.
    """

    finished = Signal(object)  # BackupRunResult
    failed = Signal(str)

    def __init__(
        self,
        *,
        profile_name: str = "default",
        data_root: Path | None = None,
        default_compression: str = "none",
    ) -> None:
        """
        Initialize the worker.

        Parameters
        ----------
        profile_name : str, optional
            Profile name used to resolve authoritative job-backed policy.
        data_root : Path | None, optional
            Optional data root override for the profile store and backup service.
        default_compression : str, optional
            GUI-level fallback compression used only when plan mode needs to
            preserve legacy OZ0 behavior for jobs whose Template policy still
            resolves to ``"none"`` in the active GUI context.
        """
        super().__init__()
        self._profile_name = profile_name
        self._data_root = data_root
        self._default_compression = default_compression
        self._job_id: str | None = None
        self._job_name: str | None = None
        self._source: Path | None = None
        self._mode: str = "plan"
        self._backup_note: str = ""

    def configure(
        self,
        job_id: str,
        job_name: str,
        source: Path,
        mode: str,
        backup_note: str = "",
        *,
        data_root: Path | None,
        default_compression: str,
    ) -> None:
        """
        Configure parameters for the next backup execution.

        Notes
        -----
        This method performs no I/O. It stores parameters for the next `run()` call.
        """
        self._job_id = job_id
        self._job_name = job_name
        self._source = source
        self._mode = mode
        self._backup_note = backup_note
        self._data_root = data_root
        self._default_compression = default_compression

    @Slot()
    def run(self) -> None:
        if self._job_id is None:
            self.failed.emit("No job selected.")
            return
        if self._source is None:
            self.failed.emit("No source folder selected.")
            return
        try:
            mode = self._mode

            if mode == "plan":
                compression = self._resolve_plan_compression(self._job_id)
                result = run_backup(
                    profile_name=self._profile_name,
                    source=self._source,
                    dry_run=True,
                    data_root=self._data_root,
                    write_plan=True,
                    job_id=self._job_id,
                    job_name=self._job_name,
                    backup_note=self._backup_note,
                    compression=compression,
                )
            elif mode == "materialize":
                result = run_backup(
                    profile_name=self._profile_name,
                    source=self._source,
                    dry_run=False,
                    data_root=self._data_root,
                    execute=False,
                    job_id=self._job_id,
                    job_name=self._job_name,
                    backup_note=self._backup_note,
                )
            elif mode == "execute":
                result = run_backup(
                    profile_name=self._profile_name,
                    source=self._source,
                    dry_run=False,
                    data_root=self._data_root,
                    execute=True,
                    job_id=self._job_id,
                    job_name=self._job_name,
                    backup_note=self._backup_note,
                )
            elif mode == "execute+compress":
                result = run_backup(
                    profile_name=self._profile_name,
                    source=self._source,
                    dry_run=False,
                    data_root=self._data_root,
                    execute=True,
                    compress=True,
                    compression="zip",
                    job_id=self._job_id,
                    job_name=self._job_name,
                    backup_note=self._backup_note,
                )
            else:
                raise ValueError(f"Unknown run mode: {mode!r}")

            self.finished.emit(result)

        except Exception as exc:
            self.failed.emit(str(exc))

    def _load_template_compression(self, job_id: str) -> str:
        """
        Load the authoritative Template-owned compression policy for a job.

        Parameters
        ----------
        job_id : str
            Job identifier whose current Template compression should be applied.

        Returns
        -------
        str
            Compression mode currently configured for the job.
        """
        store = open_profile_store(profile_name=self._profile_name, data_root=self._data_root)
        return store.load_template_compression(job_id)

    def _resolve_plan_compression(self, job_id: str) -> str:
        """
        Resolve plan-mode compression for the active GUI context.

        Parameters
        ----------
        job_id : str
            Job identifier whose plan-mode compression should be resolved.

        Returns
        -------
        str
            Compression mode that should drive plan rendering.

        Notes
        -----
        The Run tab is a user-facing planning surface and historically exposed a
        GUI default compression control. The active GUI path must continue to
        honor that OZ0 expectation when the current Template policy still
        resolves to ``"none"`` in the selected GUI context.
        """
        compression = self._load_template_compression(job_id)
        if compression != "none":
            return compression
        return self._default_compression


class RunTab(QWidget):
    """
    Run tab for the WCBT GUI.

    Responsibilities
    ----------------
    - Select a job and source folder for backup execution.
    - Dispatch backup runs via a background worker thread.
    - Display execution status and summary artifacts.

    Notes
    -----
    - This tab performs dry-run backups by default.
    - All execution is engine-backed; no CLI calls are made.
    """

    def __init__(self) -> None:
        super().__init__()
        self._settings = load_gui_settings(data_root=None)
        self._loading_settings = True
        self._active_job_id: str | None = None
        self._current_job_binding: JobBinding | None = None
        self._pending_select_job_id: str | None = None
        self._store = ProfileStoreAdapter(
            profile_name="default",
            data_root=self._settings.data_root,
        )
        self._store.jobs_loaded.connect(self._on_jobs_loaded)
        self._store.error.connect(self._on_store_error)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)

        job_box = QGroupBox("Job")
        job_layout = QHBoxLayout(job_box)
        job_stack = QVBoxLayout()
        job_layout.addLayout(job_stack, 1)

        self.job_combo = QComboBox()
        self.job_combo.setEnabled(False)
        self.job_combo.currentIndexChanged.connect(self._on_job_changed)
        self.btn_new_job = QPushButton("New Job…")
        self.btn_new_job.clicked.connect(self._new_job)
        self.btn_edit_job = QPushButton("Edit Job…")
        self.btn_edit_job.clicked.connect(self._edit_job)
        self.btn_edit_job.setEnabled(False)

        job_select_row = QHBoxLayout()
        job_select_row.addWidget(QLabel("Selected job:"))
        job_select_row.addWidget(self.job_combo, 1)
        job_select_row.addWidget(self.btn_new_job)
        job_select_row.addWidget(self.btn_edit_job)
        job_stack.addLayout(job_select_row)

        self.source_edit = QLineEdit()
        self.source_edit.setPlaceholderText("Choose source folder to back up…")

        self.btn_browse_source = QPushButton("Browse…")
        self.btn_browse_source.clicked.connect(self._browse_source)

        source_row = QHBoxLayout()
        source_row.addWidget(QLabel("Source folder:"))
        source_row.addWidget(self.source_edit, 1)
        source_row.addWidget(self.btn_browse_source)
        job_stack.addLayout(source_row)
        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Plan only (no side effects)", "plan")
        self.mode_combo.addItem("Materialize (create run + manifest, no copy)", "materialize")
        self.mode_combo.addItem("Execute (copy files into run)", "execute")
        self.mode_combo.addItem("Execute + Compress (copy files, then archive)", "execute+compress")

        run_mode_row = QHBoxLayout()
        run_mode_row.addWidget(QLabel("Run mode:"))
        run_mode_row.addWidget(self.mode_combo, 1)
        job_stack.addLayout(run_mode_row)

        self.backup_note_edit = QLineEdit()
        self.backup_note_edit.setPlaceholderText("Optional note for this backup run")
        job_stack.addLayout(self._build_backup_note_row())

        self._apply_default_run_mode()
        self.mode_combo.currentIndexChanged.connect(self._on_run_mode_changed)
        self._loading_settings = False

        self.btn_backup_now = QPushButton("Backup Now")
        self.btn_backup_now.clicked.connect(self._backup_now)

        self.btn_open_artifacts = QPushButton("Open Artifacts Folder")
        self.btn_open_artifacts.clicked.connect(self._open_artifacts)

        job_layout.addWidget(self.btn_backup_now)
        job_layout.addWidget(self.btn_open_artifacts)

        layout.addWidget(job_box)

        status_box = QGroupBox("Status")
        status_layout = QVBoxLayout(status_box)

        self.status_label = QLabel("Ready.")
        self.status_label.setStyleSheet("padding: 6px;")
        status_layout.addWidget(self.status_label)

        self.summary = QPlainTextEdit()
        self.summary.setReadOnly(True)
        self.summary.setFont(_mono())
        self.summary.setPlainText(
            "Last run summary will appear here.\n\n"
            "Fields:\n"
            "- run_id\n"
            "- status\n"
            "- candidates/selected/copied/skipped\n"
            "- artifact_root\n"
        )

        self._last_result: BackupRunResult | None = None

        self._thread = QThread(self)
        self._worker = BackupWorker(
            profile_name="default",
            data_root=self._settings.data_root,
            default_compression=self._settings.default_compression,
        )
        self._worker.moveToThread(self._thread)
        self._worker.finished.connect(self._on_backup_finished)
        self._worker.failed.connect(self._on_backup_failed)
        self._thread.start()

        status_layout.addWidget(self.summary, 1)

        layout.addWidget(status_box, 1)

        try:
            self._store.request_list_jobs.emit()
        except Exception:
            self._store.shutdown()
            raise

    def _browse_source(self) -> None:
        start_dir = self.source_edit.text().strip() or str(Path.home())
        directory = QFileDialog.getExistingDirectory(self, "Select source folder", start_dir)
        if directory:
            self.source_edit.setText(directory)

    def _apply_default_run_mode(self) -> None:
        wanted = getattr(self, "_settings", None)
        if wanted is None:
            return
        mode = wanted.default_run_mode
        for i in range(self.mode_combo.count()):
            if str(self.mode_combo.itemData(i)) == mode:
                self.mode_combo.setCurrentIndex(i)
                return

    def _on_run_mode_changed(self, *_args: object) -> None:
        """
        Persist the currently selected Run tab mode as a GUI preference.
        """
        if getattr(self, "_loading_settings", False):
            return

        selected_mode = str(self.mode_combo.currentData())
        updated_settings = GuiSettings(
            data_root=self._settings.data_root,
            archives_root=self._settings.archives_root,
            default_compression=self._settings.default_compression,
            default_run_mode=selected_mode,
            restore_mode=self._settings.restore_mode,
            restore_verify=self._settings.restore_verify,
            restore_dry_run=self._settings.restore_dry_run,
            pre_restore_backup_compression=self._settings.pre_restore_backup_compression,
        )
        save_gui_settings(data_root=None, settings=updated_settings)
        self._settings = updated_settings

    def _selected_job_id(self) -> str | None:
        if self.job_combo.currentIndex() < 0:
            return None
        return str(self.job_combo.currentData())

    def _refresh_settings_context(self) -> None:
        """
        Reload the active GUI settings for runtime-sensitive Run tab actions.

        Notes
        -----
        The Settings tab can change the default compression while the app is
        already open. Run-tab actions must refresh that context so Plan Only and
        other OZ0 flows do not keep using a stale legacy archive-root fallback.
        """
        self._settings = load_gui_settings(data_root=None)

    def _open_store(self):
        self._refresh_settings_context()
        return open_profile_store(profile_name="default", data_root=self._settings.data_root)

    def _refresh_job_binding(self, job_id: str | None) -> None:
        if job_id is None:
            self._current_job_binding = None
            self.source_edit.setText("")
            self.btn_edit_job.setEnabled(False)
            return

        try:
            binding = self._open_store().load_job_binding(job_id)
        except Exception as exc:
            self._current_job_binding = None
            self.source_edit.setText("")
            self.btn_edit_job.setEnabled(False)
            self.status_label.setText("Error")
            QMessageBox.critical(self, "Profile Store Error", str(exc))
            return

        self._current_job_binding = binding
        self.source_edit.setText(binding.source_root)
        self.btn_edit_job.setEnabled(True)

    def _template_choices(self) -> list[_TemplateChoice]:
        store = self._open_store()
        choices: list[_TemplateChoice] = [_TemplateChoice("Create new template", None)]
        for summary in store.list_jobs():
            binding = store.load_job_binding(summary.job_id)
            choices.append(
                _TemplateChoice(
                    f"Reuse template from '{binding.job_name}'",
                    binding.template_id,
                )
            )
        return choices

    def _persist_current_source_root_if_needed(self, source_root: str) -> None:
        binding = self._current_job_binding
        if binding is None or binding.source_root == source_root:
            return

        updated_binding = JobBinding(
            job_id=binding.job_id,
            job_name=binding.job_name,
            template_id=binding.template_id,
            source_root=source_root,
        )
        self._open_store().save_job_binding(updated_binding)
        self._current_job_binding = updated_binding

    def _backup_now(self) -> None:
        binding = self._current_job_binding
        if binding is None:
            QMessageBox.information(self, "Backup", "Select a job first.")
            return

        source_text = self.source_edit.text().strip()
        if not source_text:
            QMessageBox.information(self, "Backup", "Choose a source folder first.")
            return

        source = Path(source_text)
        if not source.exists() or not source.is_dir():
            QMessageBox.critical(self, "Backup", "Source folder does not exist.")
            return

        try:
            self._persist_current_source_root_if_needed(source_text)
        except Exception as exc:
            QMessageBox.critical(self, "Profile Store Error", str(exc))
            return

        self._refresh_settings_context()

        self.btn_backup_now.setEnabled(False)
        mode = str(self.mode_combo.currentData())
        action = {
            "plan": "Planning",
            "materialize": "Materializing",
            "execute": "Executing",
            "execute+compress": "Executing + Compressing",
        }.get(mode, "Running")

        self.status_label.setText(f"{action}: {self.job_combo.currentText()} …")
        self.summary.setPlainText(f"{action} backup…")

        self._worker.configure(
            binding.job_id,
            binding.job_name,
            source,
            mode,
            self.backup_note_edit.text(),
            data_root=self._settings.data_root,
            default_compression=self._settings.default_compression,
        )
        QMetaObject.invokeMethod(self._worker, "run", Qt.ConnectionType.QueuedConnection)

    def _build_backup_note_row(self) -> QHBoxLayout:
        """
        Build the optional backup note input row.

        Returns
        -------
        QHBoxLayout
            Layout containing the backup note controls.
        """
        note_row = QHBoxLayout()
        note_row.addWidget(QLabel("Backup note:"))
        note_row.addWidget(self.backup_note_edit, 1)
        return note_row

    def _open_artifacts(self) -> None:
        binding = self._current_job_binding
        if binding is None or not binding.source_root.strip():
            QMessageBox.information(
                self, "Artifacts", "Select a job with a saved source folder first."
            )
            return

        root = resolve_oz0_artifact_root(Path(binding.source_root))
        root.mkdir(parents=True, exist_ok=True)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(root)))

    def _on_backup_finished(self, result_obj: object) -> None:
        result: BackupRunResult = result_obj  # engine type
        self._last_result = result
        self.btn_backup_now.setEnabled(True)

        self.status_label.setText(f"Completed: {self.job_combo.currentText()}")
        self.summary.setPlainText(result.report_text)

    def _on_backup_failed(self, message: str) -> None:
        self._last_result = None
        self.btn_backup_now.setEnabled(True)
        self.status_label.setText("Error")
        QMessageBox.critical(self, "Backup failed", message)

    def _on_jobs_loaded(self, jobs_obj: object) -> None:
        try:
            jobs = list(jobs_obj)
        except Exception:
            QMessageBox.critical(self, "Profile Store Error", "Invalid job list from store.")
            return

        self.job_combo.blockSignals(True)
        try:
            self.job_combo.clear()
            for js in jobs:
                job_id = str(getattr(js, "job_id"))
                name = str(getattr(js, "name"))
                self.job_combo.addItem(name, job_id)
        finally:
            self.job_combo.blockSignals(False)

        if self.job_combo.count() == 0:
            self.job_combo.setEnabled(False)
            self.status_label.setText("No jobs yet. Create one here or in Authoring.")
            self.btn_backup_now.setEnabled(False)
            self.btn_edit_job.setEnabled(False)
            self._current_job_binding = None
            return

        self.job_combo.setEnabled(True)
        self.btn_backup_now.setEnabled(True)
        if self._pending_select_job_id is not None:
            target = self._pending_select_job_id
            self._pending_select_job_id = None
            for index in range(self.job_combo.count()):
                if str(self.job_combo.itemData(index)) == target:
                    self.job_combo.setCurrentIndex(index)
                    break
        self._on_job_changed()

    def _on_store_error(self, job_id: str, message: str) -> None:
        self.status_label.setText("Error")
        QMessageBox.critical(self, "Profile Store Error", message)

    def _on_job_changed(self) -> None:
        self._active_job_id = self._selected_job_id()
        self._refresh_job_binding(self._active_job_id)

    def _job_name_exists(self, name: str, *, excluding_job_id: str | None = None) -> bool:
        name_lower = name.strip().lower()
        for index in range(self.job_combo.count()):
            job_id = str(self.job_combo.itemData(index))
            if excluding_job_id is not None and job_id == excluding_job_id:
                continue
            if str(self.job_combo.itemText(index)).strip().lower() == name_lower:
                return True
        return False

    def _new_job(self) -> None:
        try:
            template_choices = self._template_choices()
        except Exception as exc:
            QMessageBox.critical(self, "Profile Store Error", str(exc))
            return

        dialog = JobBindingDialog(
            self,
            title="New Job",
            initial_name="",
            initial_source_root=self.source_edit.text().strip(),
            template_choices=template_choices,
            initial_template_id=None,
        )
        if dialog.exec() != QDialog.Accepted:
            return

        name = dialog.selected_name()
        if not name:
            QMessageBox.warning(self, "New Job", "Job name cannot be empty.")
            return
        if self._job_name_exists(name):
            QMessageBox.warning(self, "New Job", "A job with that name already exists.")
            return

        try:
            store = self._open_store()
            job_id = store.create_job(name)
            created_binding = store.load_job_binding(job_id)
            store.save_job_binding(
                JobBinding(
                    job_id=job_id,
                    job_name=name,
                    template_id=dialog.selected_template_id() or created_binding.template_id,
                    source_root=dialog.selected_source_root(),
                )
            )
        except Exception as exc:
            QMessageBox.critical(self, "Profile Store Error", str(exc))
            return

        self._pending_select_job_id = job_id
        self._store.request_list_jobs.emit()

    def _edit_job(self) -> None:
        binding = self._current_job_binding
        if binding is None:
            QMessageBox.information(self, "Edit Job", "Select a job first.")
            return

        try:
            template_choices = self._template_choices()
        except Exception as exc:
            QMessageBox.critical(self, "Profile Store Error", str(exc))
            return

        dialog = JobBindingDialog(
            self,
            title="Edit Job",
            initial_name=binding.job_name,
            initial_source_root=self.source_edit.text().strip() or binding.source_root,
            template_choices=template_choices,
            initial_template_id=binding.template_id,
        )
        if dialog.exec() != QDialog.Accepted:
            return

        name = dialog.selected_name()
        if not name:
            QMessageBox.warning(self, "Edit Job", "Job name cannot be empty.")
            return
        if self._job_name_exists(name, excluding_job_id=binding.job_id):
            QMessageBox.warning(self, "Edit Job", "A job with that name already exists.")
            return

        try:
            updated_binding = JobBinding(
                job_id=binding.job_id,
                job_name=name,
                template_id=dialog.selected_template_id() or binding.template_id,
                source_root=dialog.selected_source_root(),
            )
            self._open_store().save_job_binding(updated_binding)
        except UnknownJobError:
            QMessageBox.critical(self, "Edit Job", "The selected job no longer exists.")
            return
        except Exception as exc:
            QMessageBox.critical(self, "Profile Store Error", str(exc))
            return

        self._current_job_binding = updated_binding
        self._pending_select_job_id = updated_binding.job_id
        self._store.request_list_jobs.emit()

    def shutdown(self) -> None:
        """
        Shut down background worker threads and the ProfileStore adapter.

        Notes
        -----
        This method is safe to call multiple times.
        """
        self._thread.quit()
        self._thread.wait(2000)
        self._store.shutdown()

    def closeEvent(self, event) -> None:
        try:
            self._thread.quit()
            self._thread.wait(2000)
        finally:
            self._store.shutdown()
            super().closeEvent(event)
