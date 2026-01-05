"""
Run tab mock.

This is UI only. No WCBT engine integration.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from PySide6.QtGui import QFont
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

from gui.mock_data import MockJob


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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
        self.btn_backup_now.clicked.connect(self._mock_backup_now)

        self.btn_open_artifacts = QPushButton("Open Artifacts Folder")
        self.btn_open_artifacts.clicked.connect(self._mock_open_artifacts)

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
        status_layout.addWidget(self.summary, 1)

        layout.addWidget(status_box, 1)

    def _selected_job(self) -> MockJob:
        job_id = str(self.job_combo.currentData())
        return next(j for j in self._jobs if j.job_id == job_id)

    def _mock_backup_now(self) -> None:
        job = self._selected_job()
        now = datetime.now()
        run_id = f"run-{job.job_id[-3:]}-MOCK"
        artifact_root = Path(r"C:\wcbt_data\artifacts") / job.job_id / "run_mock"

        self.status_label.setText(f"Completed (mock): {job.name}")
        self.summary.setPlainText(
            "\n".join(
                [
                    f"run_id: {run_id}",
                    "status: success",
                    "dry_run: false",
                    f"job: {job.name}",
                    f"root: {job.root_path}",
                    f"started_at: {_format_dt(now)}",
                    f"finished_at: {_format_dt(now + timedelta(seconds=2))}",
                    "counts:",
                    "  candidates: 18234",
                    "  selected: 18234",
                    "  copied: 120",
                    "  skipped: 18114",
                    "  conflicts: 0",
                    "  unreadable: 0",
                    "  hash_mismatch: 0",
                    f"artifact_root: {artifact_root}",
                ]
            )
        )

    def _mock_open_artifacts(self) -> None:
        QMessageBox.information(
            self, "Mock", "This would open the artifacts folder for the selected job."
        )
