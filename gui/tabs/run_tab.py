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
    QFileDialog,
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
from gui.adapters.profile_store_adapter import ProfileStoreAdapter
from gui.settings_store import load_gui_settings


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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

    def __init__(self) -> None:
        super().__init__()
        self._job_id: str | None = None
        self._source: Path | None = None
        self._mode: str = "plan"

    def configure(self, job_id: str, source: Path, mode: str) -> None:
        """
        Configure parameters for the next backup execution.

        Notes
        -----
        This method performs no I/O. It stores parameters for the next `run()` call.
        """
        self._job_id = job_id
        self._source = source
        self._mode = mode

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
                result = run_backup(
                    profile_name="default",
                    source=self._source,
                    dry_run=True,
                    data_root=None,
                    write_plan=True,
                )
            elif mode == "materialize":
                result = run_backup(
                    profile_name="default",
                    source=self._source,
                    dry_run=False,
                    data_root=None,
                    execute=False,
                )
            elif mode == "execute":
                result = run_backup(
                    profile_name="default",
                    source=self._source,
                    dry_run=False,
                    data_root=None,
                    execute=True,
                )
            elif mode == "execute+compress":
                result = run_backup(
                    profile_name="default",
                    source=self._source,
                    dry_run=False,
                    data_root=None,
                    execute=True,
                    compress=True,
                    compression="zip",
                )
            else:
                raise ValueError(f"Unknown run mode: {mode!r}")

            self.finished.emit(result)

        except Exception as exc:
            self.failed.emit(str(exc))


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
        self._active_job_id: str | None = None
        self._store = ProfileStoreAdapter(profile_name="default", data_root=None)
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

        job_select_row = QHBoxLayout()
        job_select_row.addWidget(QLabel("Selected job:"))
        job_select_row.addWidget(self.job_combo, 1)
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

        self._settings = load_gui_settings(data_root=None)
        self._apply_default_run_mode()

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
        self._worker = BackupWorker()
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

    def _selected_job_id(self) -> str | None:
        if self.job_combo.currentIndex() < 0:
            return None
        return str(self.job_combo.currentData())

    def _backup_now(self) -> None:
        job_id = self._selected_job_id()
        if job_id is None:
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

        self._worker.configure(job_id, source, mode)
        QMetaObject.invokeMethod(self._worker, "run", Qt.ConnectionType.QueuedConnection)

    def _open_artifacts(self) -> None:
        if self._last_result is None:
            QMessageBox.information(self, "Artifacts", "Run a backup first.")
            return

        root = self._last_result.archive_root
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
            self.status_label.setText("No jobs yet. Create one in Authoring.")
            self.btn_backup_now.setEnabled(False)
            return

        self.job_combo.setEnabled(True)
        self.btn_backup_now.setEnabled(True)
        self._on_job_changed()

    def _on_store_error(self, job_id: str, message: str) -> None:
        self.status_label.setText("Error")
        QMessageBox.critical(self, "Profile Store Error", message)

    def _on_job_changed(self) -> None:
        self._active_job_id = self._selected_job_id()

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
