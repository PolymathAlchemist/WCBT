"""
Run tab mock.

This is UI only. No WCBT engine integration.
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
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from backup_engine.backup.service import BackupRunResult, run_backup
from gui.mock_data import MockJob


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class BackupWorker(QObject):
    finished = Signal(object)  # BackupRunResult
    failed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._job: MockJob | None = None

    def configure(self, job: MockJob) -> None:
        self._job = job

    @Slot()
    def run(self) -> None:
        if self._job is None:
            self.failed.emit("No job selected.")
            return
        try:
            # Safe default: plan-only but write plan artifact so UI can open it.
            result = run_backup(
                profile_name="default",
                source=Path(self._job.root_path),
                dry_run=True,
                data_root=None,
                write_plan=True,
            )
            self.finished.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class RunTab(QWidget):
    def __init__(self, jobs: list[MockJob]) -> None:
        super().__init__()
        self._jobs = jobs

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)

        job_box = QGroupBox("Job")
        job_layout = QHBoxLayout(job_box)

        self.job_combo = QComboBox()
        for job in jobs:
            self.job_combo.addItem(job.name, job.job_id)

        self.btn_backup_now = QPushButton("Backup Now")
        self.btn_backup_now.clicked.connect(self._backup_now)

        self.btn_open_artifacts = QPushButton("Open Artifacts Folder")
        self.btn_open_artifacts.clicked.connect(self._open_artifacts)

        job_layout.addWidget(QLabel("Selected job:"))
        job_layout.addWidget(self.job_combo, 1)
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
            "Mock fields:\n"
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

    def _selected_job(self) -> MockJob:
        job_id = str(self.job_combo.currentData())
        return next(j for j in self._jobs if j.job_id == job_id)

    def _backup_now(self) -> None:
        job = self._selected_job()
        self.btn_backup_now.setEnabled(False)
        self.status_label.setText(f"Planning: {job.name} …")
        self.summary.setPlainText("Running backup plan…")
        self._worker.configure(job)
        QThread.msleep(0)  # no-op, keeps intent explicit
        # queued invocation by posting to worker thread:
        QMetaObject.invokeMethod(self._worker, "run", Qt.ConnectionType.QueuedConnection)

    def _open_artifacts(self) -> None:
        if self._last_result is None:
            QMessageBox.information(self, "Artifacts", "Run a backup first.")
            return

        root = self._last_result.archive_root
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(root)))

    def closeEvent(self, event) -> None:
        self._thread.quit()
        self._thread.wait(2000)
        super().closeEvent(event)

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
