"""
WCBT GUI mock (PySide6).

This is a UI/UX prototype only:
- No integration with WCBT core
- No filesystem scanning
- Uses in-memory mock jobs and runs

Purpose:
- Iterate quickly on layout and information hierarchy
- Validate admin (authoring) vs user (run/restore) flow
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)


@dataclass(frozen=True, slots=True)
class MockJob:
    job_id: str
    name: str
    root_path: Path


@dataclass(frozen=True, slots=True)
class MockRun:
    run_id: str
    job_id: str
    started_at: datetime
    finished_at: datetime
    status: str  # "success" | "failed"
    dry_run: bool
    artifact_root: Path
    manifest_summary: dict[str, str]
    counts_summary: dict[str, int]


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class MockWcbtGui(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("WCBT GUI Mock (No Engine Wiring)")
        self.resize(1180, 720)

        self._jobs = self._seed_jobs()
        self._runs = self._seed_runs()

        root = QVBoxLayout(self)
        root.addWidget(self._build_header())
        root.addWidget(self._build_tabs())

    # ---------- Seed data ----------

    def _seed_jobs(self) -> list[MockJob]:
        return [
            MockJob("job-001", "Minecraft Server Backup", Path(r"C:\dev\minecraft_server")),
            MockJob("job-002", "Photos Archive", Path(r"D:\photos")),
            MockJob("job-003", "WCBT Repo Snapshot", Path(r"C:\dev\wcbt")),
        ]

    def _seed_runs(self) -> list[MockRun]:
        now = datetime.now()
        runs: list[MockRun] = []

        def add_run(i: int, job_id: str, status: str, dry_run: bool) -> None:
            started = now - timedelta(days=i, hours=1)
            finished = started + timedelta(minutes=3, seconds=12)
            runs.append(
                MockRun(
                    run_id=f"run-{job_id[-3:]}-{1000 + i}",
                    job_id=job_id,
                    started_at=started,
                    finished_at=finished,
                    status=status,
                    dry_run=dry_run,
                    artifact_root=Path(r"C:\wcbt_data\artifacts") / job_id / f"run_{1000 + i}",
                    manifest_summary={
                        "schema_version": "1",
                        "source_root": str(
                            Path(r"C:\dev\minecraft_server")
                            if job_id == "job-001"
                            else Path(r"C:\dev\wcbt")
                        ),
                        "hash_alg": "sha256",
                        "candidate_rule": "include-all; excludes=['logs/**']",
                    },
                    counts_summary={
                        "candidates": 18234 + i * 3,
                        "selected": 18234 + i * 3,
                        "excluded": 0,
                        "copied": 120 + i,
                        "skipped": 18114,
                        "conflicts": 0,
                        "unreadable": 0,
                        "hash_mismatch": 0 if status == "success" else 2,
                    },
                )
            )

        # Minecraft job runs
        add_run(1, "job-001", "success", False)
        add_run(2, "job-001", "success", False)
        add_run(3, "job-001", "failed", False)
        add_run(4, "job-001", "success", True)

        # WCBT job runs
        add_run(1, "job-003", "success", False)
        add_run(2, "job-003", "success", False)

        return runs

    # ---------- UI construction ----------

    def _build_header(self) -> QWidget:
        w = QWidget()
        layout = QHBoxLayout(w)
        layout.setContentsMargins(8, 8, 8, 8)

        title = QLabel("WCBT")
        f = title.font()
        f.setPointSize(16)
        f.setBold(True)
        title.setFont(f)

        subtitle = QLabel("GUI mock for UX iteration (no core wiring)")
        subtitle.setStyleSheet("color: #666;")

        layout.addWidget(title)
        layout.addSpacing(10)
        layout.addWidget(subtitle)
        layout.addStretch(1)

        return w

    def _build_tabs(self) -> QTabWidget:
        tabs = QTabWidget()
        tabs.addTab(self._build_run_tab(), "Run")
        tabs.addTab(self._build_restore_tab(), "Restore")
        tabs.addTab(self._build_authoring_tab(), "Authoring")
        return tabs

    # ---------- Run tab ----------

    def _build_run_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(12, 12, 12, 12)

        job_box = QGroupBox("Job")
        job_layout = QHBoxLayout(job_box)
        self.run_job_combo = QComboBox()
        for job in self._jobs:
            self.run_job_combo.addItem(job.name, job.job_id)

        job_layout.addWidget(QLabel("Selected job:"))
        job_layout.addWidget(self.run_job_combo, 1)

        self.btn_backup_now = QPushButton("Backup Now")
        self.btn_backup_now.clicked.connect(self._mock_backup_now)
        self.btn_view_artifacts = QPushButton("Open Artifacts Folder")
        self.btn_view_artifacts.clicked.connect(self._mock_open_artifacts)

        job_layout.addWidget(self.btn_backup_now)
        job_layout.addWidget(self.btn_view_artifacts)

        layout.addWidget(job_box)

        status_box = QGroupBox("Status")
        status_layout = QVBoxLayout(status_box)

        self.run_status_label = QLabel("Ready.")
        self.run_status_label.setStyleSheet("padding: 6px;")
        status_layout.addWidget(self.run_status_label)

        self.run_last_summary = QPlainTextEdit()
        self.run_last_summary.setReadOnly(True)
        self.run_last_summary.setFont(_mono())
        self.run_last_summary.setPlainText(
            "Last run summary will appear here.\n\n"
            "Mock fields:\n"
            "- run_id\n"
            "- status\n"
            "- candidates/selected/copied/skipped\n"
            "- artifact_root\n"
        )
        status_layout.addWidget(self.run_last_summary, 1)

        layout.addWidget(status_box, 1)

        return w

    def _mock_backup_now(self) -> None:
        job_id = str(self.run_job_combo.currentData())
        job = next(j for j in self._jobs if j.job_id == job_id)

        # Mock run result
        now = datetime.now()
        run_id = f"run-{job_id[-3:]}-MOCK"
        artifact_root = Path(r"C:\wcbt_data\artifacts") / job_id / "run_mock"

        self.run_status_label.setText(f"Completed (mock): {job.name}")
        self.run_last_summary.setPlainText(
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

    # ---------- Restore tab ----------

    def _build_restore_tab(self) -> QWidget:
        w = QWidget()
        outer = QVBoxLayout(w)
        outer.setContentsMargins(12, 12, 12, 12)

        top = QWidget()
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 0)

        self.restore_job_combo = QComboBox()
        for job in self._jobs:
            self.restore_job_combo.addItem(job.name, job.job_id)
        self.restore_job_combo.currentIndexChanged.connect(self._refresh_restore_history)

        self.restore_filter = QLineEdit()
        self.restore_filter.setPlaceholderText("Filter runs (status, run id, etc.)")
        self.restore_filter.textChanged.connect(self._refresh_restore_history)

        top_layout.addWidget(QLabel("Job:"))
        top_layout.addWidget(self.restore_job_combo)
        top_layout.addSpacing(10)
        top_layout.addWidget(self.restore_filter, 1)

        outer.addWidget(top)

        splitter = QSplitter(Qt.Horizontal)

        # Left: history list
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.history_list = QListWidget()
        self.history_list.currentItemChanged.connect(self._on_history_selected)
        left_layout.addWidget(QLabel("History"))
        left_layout.addWidget(self.history_list, 1)

        # Right: details
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)

        right_layout.addWidget(QLabel("Selected run details"))

        self.run_details = QPlainTextEdit()
        self.run_details.setReadOnly(True)
        self.run_details.setFont(_mono())
        right_layout.addWidget(self.run_details, 2)

        links = QGroupBox("Artifacts (mock links)")
        links_layout = QHBoxLayout(links)
        for label in [
            "Open artifacts",
            "Open candidate list",
            "Open manifest",
            "Open verify report",
            "Open journal",
        ]:
            btn = QPushButton(label)
            btn.clicked.connect(
                btn.clicked.connect(
                    lambda _=False, label_text=label: QMessageBox.information(
                        self, "Mock", label_text
                    )
                )
            )
            links_layout.addWidget(btn)
        right_layout.addWidget(links)

        restore_box = QGroupBox("Restore (mock)")
        restore_layout = QFormLayout(restore_box)

        self.restore_dest = QLineEdit()
        self.btn_pick_dest = QPushButton("Browse…")
        self.btn_pick_dest.clicked.connect(self._pick_restore_dest)

        dest_row = QWidget()
        dest_row_layout = QHBoxLayout(dest_row)
        dest_row_layout.setContentsMargins(0, 0, 0, 0)
        dest_row_layout.addWidget(self.restore_dest, 1)
        dest_row_layout.addWidget(self.btn_pick_dest)
        restore_layout.addRow("Destination:", dest_row)

        self.restore_dry_run = QCheckBox("Dry run")
        self.restore_verify_after = QCheckBox("Verify after restore")
        self.restore_verify_after.setChecked(True)

        opts_row = QWidget()
        opts_layout = QHBoxLayout(opts_row)
        opts_layout.setContentsMargins(0, 0, 0, 0)
        opts_layout.addWidget(self.restore_dry_run)
        opts_layout.addWidget(self.restore_verify_after)
        opts_layout.addStretch(1)
        restore_layout.addRow("Options:", opts_row)

        self.btn_restore = QPushButton("Restore Selected Run")
        self.btn_restore.clicked.connect(self._mock_restore)
        restore_layout.addRow("", self.btn_restore)

        right_layout.addWidget(restore_box, 1)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([360, 820])

        outer.addWidget(splitter, 1)

        self._refresh_restore_history()
        return w

    def _refresh_restore_history(self) -> None:
        job_id = str(self.restore_job_combo.currentData())
        needle = self.restore_filter.text().strip().lower()

        items = [r for r in self._runs if r.job_id == job_id]
        items.sort(key=lambda r: r.started_at, reverse=True)

        self.history_list.clear()
        for r in items:
            text = f"{_format_dt(r.started_at)}  {r.status.upper()}  {r.run_id}"
            if needle and needle not in text.lower():
                continue

            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, r.run_id)
            if r.status == "failed":
                item.setForeground(Qt.red)
            self.history_list.addItem(item)

        if self.history_list.count() > 0:
            self.history_list.setCurrentRow(0)
        else:
            self.run_details.setPlainText("No runs found for this job (with current filter).")

    def _on_history_selected(
        self, current: QListWidgetItem | None, _prev: QListWidgetItem | None
    ) -> None:
        if current is None:
            return
        run_id = str(current.data(Qt.UserRole))
        run = next(r for r in self._runs if r.run_id == run_id)

        details = []
        details.append("RUN SUMMARY")
        details.append(f"  run_id: {run.run_id}")
        details.append(f"  status: {run.status}")
        details.append(f"  dry_run: {str(run.dry_run).lower()}")
        details.append(f"  started_at: {_format_dt(run.started_at)}")
        details.append(f"  finished_at: {_format_dt(run.finished_at)}")
        details.append(f"  artifact_root: {run.artifact_root}")
        details.append("")
        details.append("MANIFEST SUMMARY (mock)")
        for k, v in run.manifest_summary.items():
            details.append(f"  {k}: {v}")
        details.append("")
        details.append("COUNTS (mock)")
        for k, v in run.counts_summary.items():
            details.append(f"  {k}: {v}")

        self.run_details.setPlainText("\n".join(details))

    def _pick_restore_dest(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select restore destination")
        if d:
            self.restore_dest.setText(d)

    def _mock_restore(self) -> None:
        cur = self.history_list.currentItem()
        if cur is None:
            QMessageBox.warning(self, "Restore", "Select a run to restore.")
            return

        dest = self.restore_dest.text().strip()
        if not dest:
            QMessageBox.warning(self, "Restore", "Choose a destination folder.")
            return

        msg = (
            "This is a mock restore.\n\n"
            f"Destination: {dest}\n"
            f"Dry run: {self.restore_dry_run.isChecked()}\n"
            f"Verify after: {self.restore_verify_after.isChecked()}\n"
        )
        QMessageBox.information(self, "Mock", msg)

    # ---------- Authoring tab ----------

    def _build_authoring_tab(self) -> QWidget:
        w = QWidget()
        outer = QVBoxLayout(w)
        outer.setContentsMargins(12, 12, 12, 12)

        job_box = QGroupBox("Job definition (mock)")
        job_layout = QFormLayout(job_box)

        self.auth_name = QLineEdit("Minecraft Server Backup")
        job_layout.addRow("Name:", self.auth_name)

        self.auth_root = QLineEdit(r"C:\dev\minecraft_server")
        self.btn_auth_root = QPushButton("Browse…")
        self.btn_auth_root.clicked.connect(self._pick_authoring_root)

        root_row = QWidget()
        root_row_layout = QHBoxLayout(root_row)
        root_row_layout.setContentsMargins(0, 0, 0, 0)
        root_row_layout.addWidget(self.auth_root, 1)
        root_row_layout.addWidget(self.btn_auth_root)
        job_layout.addRow("Root folder:", root_row)

        self.auth_locked = QCheckBox("Lock job (prevent accidental edits)")
        job_layout.addRow("", self.auth_locked)

        outer.addWidget(job_box)

        rules = QGroupBox("Include / exclude rules (mock)")
        rules_layout = QHBoxLayout(rules)

        self.include_patterns = QPlainTextEdit()
        self.include_patterns.setFont(_mono())
        self.include_patterns.setPlainText(
            "\n".join(
                [
                    "mods/**",
                    "config/**",
                    "libraries/**",
                    "Lighthouse_Archipelago/**",
                    "*.json",
                    "*.properties",
                    "*.jar",
                ]
            )
        )

        self.exclude_patterns = QPlainTextEdit()
        self.exclude_patterns.setFont(_mono())
        self.exclude_patterns.setPlainText("\n".join(["logs/**", "crash-reports/**"]))

        inc_box = QGroupBox("Include patterns")
        inc_layout = QVBoxLayout(inc_box)
        inc_layout.addWidget(self.include_patterns)

        exc_box = QGroupBox("Exclude patterns")
        exc_layout = QVBoxLayout(exc_box)
        exc_layout.addWidget(self.exclude_patterns)

        rules_layout.addWidget(inc_box, 1)
        rules_layout.addWidget(exc_box, 1)

        outer.addWidget(rules, 2)

        preview_box = QGroupBox("Preview (mock)")
        preview_layout = QVBoxLayout(preview_box)

        self.preview_table = QTableWidget(0, 3)
        self.preview_table.setHorizontalHeaderLabels(["Path", "Disposition", "Reason"])
        self.preview_table.horizontalHeader().setStretchLastSection(True)
        preview_layout.addWidget(self.preview_table, 1)

        btn_row = QWidget()
        btn_layout = QHBoxLayout(btn_row)
        btn_layout.setContentsMargins(0, 0, 0, 0)

        self.btn_preview = QPushButton("Preview candidate set")
        self.btn_preview.clicked.connect(self._mock_preview)

        self.btn_save_job = QPushButton("Save job (mock)")
        self.btn_save_job.clicked.connect(
            lambda: QMessageBox.information(self, "Mock", "This would write a job spec file.")
        )

        btn_layout.addWidget(self.btn_preview)
        btn_layout.addStretch(1)
        btn_layout.addWidget(self.btn_save_job)

        preview_layout.addWidget(btn_row)

        outer.addWidget(preview_box, 2)

        self._mock_preview()
        return w

    def _pick_authoring_root(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select backup root folder")
        if d:
            self.auth_root.setText(d)

    def _mock_preview(self) -> None:
        rows = [
            (r"mods\fabric-api.jar", "include", "Matches include: mods/**"),
            (r"config\some_mod.json", "include", "Matches include: config/**"),
            (
                r"Lighthouse_Archipelago\level.dat",
                "include",
                "Matches include: Lighthouse_Archipelago/**",
            ),
            (r"logs\latest.log", "exclude", "Matches exclude: logs/**"),
            (r"crash-reports\crash-2026-01-01.txt", "exclude", "Matches exclude: crash-reports/**"),
            (r"server.properties", "include", "Matches include: *.properties"),
        ]

        self.preview_table.setRowCount(0)
        for path, disp, reason in rows:
            r = self.preview_table.rowCount()
            self.preview_table.insertRow(r)
            self.preview_table.setItem(r, 0, QTableWidgetItem(path))
            self.preview_table.setItem(r, 1, QTableWidgetItem(disp))
            self.preview_table.setItem(r, 2, QTableWidgetItem(reason))


def main() -> int:
    app = QApplication(sys.argv)
    win = MockWcbtGui()
    win.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
