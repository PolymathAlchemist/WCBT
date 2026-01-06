"""
Restore tab mock.

Lists mock run history and shows manifest-like details.
No WCBT engine integration.
"""

from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import (
    QMetaObject,
    QObject,
    Qt,
    QThread,
    Signal,
    Slot,
)
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
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
    QVBoxLayout,
    QWidget,
)

from backup_engine.restore.service import RestoreRunResult, run_restore
from gui.mock_data import MockJob, MockRun


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _format_dt(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class RestoreWorker(QObject):
    finished = Signal(object)  # RestoreRunResult
    failed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._manifest_path: Path | None = None
        self._destination_root: Path | None = None
        self._mode: str = "add-only"
        self._verify: str = "size"
        self._dry_run: bool = True

    def configure(
        self,
        *,
        manifest_path: Path,
        destination_root: Path,
        mode: str,
        verify: str,
        dry_run: bool,
    ) -> None:
        self._manifest_path = manifest_path
        self._destination_root = destination_root
        self._mode = mode
        self._verify = verify
        self._dry_run = dry_run

    @Slot()
    def run(self) -> None:
        if self._manifest_path is None or self._destination_root is None:
            self.failed.emit("Missing manifest path or destination.")
            return

        try:
            result = run_restore(
                manifest_path=self._manifest_path,
                destination_root=self._destination_root,
                mode=self._mode,
                verify=self._verify,
                dry_run=self._dry_run,
                data_root=None,
            )
            self.finished.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class RestoreTab(QWidget):
    def __init__(self, jobs: list[MockJob], runs: list[MockRun]) -> None:
        super().__init__()
        self._jobs = jobs
        self._runs = runs

        outer = QVBoxLayout(self)
        outer.setContentsMargins(12, 12, 12, 12)

        top = QWidget()
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 0)

        self.job_combo = QComboBox()
        for job in jobs:
            self.job_combo.addItem(job.name, job.job_id)
        self.job_combo.currentIndexChanged.connect(self._refresh_history)

        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Filter runs (status, run id, etc.)")
        self.filter_edit.textChanged.connect(self._refresh_history)

        top_layout.addWidget(QLabel("Job:"))
        top_layout.addWidget(self.job_combo)
        top_layout.addSpacing(10)
        top_layout.addWidget(self.filter_edit, 1)
        outer.addWidget(top)

        splitter = QSplitter(Qt.Horizontal)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.addWidget(QLabel("History"))

        self.history = QListWidget()
        self.history.currentItemChanged.connect(self._on_selected)
        left_layout.addWidget(self.history, 1)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)

        right_layout.addWidget(QLabel("Selected run details"))

        self.details = QPlainTextEdit()
        self.details.setReadOnly(True)
        self.details.setFont(_mono())
        right_layout.addWidget(self.details, 2)

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
                lambda _=False, labeled=label: QMessageBox.information(self, "Mock", labeled)
            )
            links_layout.addWidget(btn)
        right_layout.addWidget(links)

        restore_box = QGroupBox("Restore (mock)")
        restore_layout = QFormLayout(restore_box)

        self.dest = QLineEdit()
        btn_browse = QPushButton("Browse…")
        btn_browse.clicked.connect(self._pick_dest)

        dest_row = QWidget()
        dest_row_layout = QHBoxLayout(dest_row)
        dest_row_layout.setContentsMargins(0, 0, 0, 0)
        dest_row_layout.addWidget(self.dest, 1)
        dest_row_layout.addWidget(btn_browse)
        restore_layout.addRow("Destination:", dest_row)

        self.dry_run = QCheckBox("Dry run")
        self.verify_after = QCheckBox("Verify after restore")
        self.verify_after.setChecked(True)

        opts_row = QWidget()
        opts_layout = QHBoxLayout(opts_row)
        opts_layout.setContentsMargins(0, 0, 0, 0)
        opts_layout.addWidget(self.dry_run)
        opts_layout.addWidget(self.verify_after)
        opts_layout.addStretch(1)
        restore_layout.addRow("Options:", opts_row)

        btn_restore = QPushButton("Restore Selected Run")
        btn_restore.clicked.connect(self._restore_selected)
        restore_layout.addRow("", btn_restore)

        right_layout.addWidget(restore_box, 1)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([360, 820])

        outer.addWidget(splitter, 1)

        self._refresh_history()

        self._last_result: RestoreRunResult | None = None

        self._thread = QThread(self)
        self._worker = RestoreWorker()
        self._worker.moveToThread(self._thread)
        self._worker.finished.connect(self._on_restore_finished)
        self._worker.failed.connect(self._on_restore_failed)
        self._thread.start()

    def _selected_job_id(self) -> str:
        return str(self.job_combo.currentData())

    def _refresh_history(self) -> None:
        job_id = self._selected_job_id()
        needle = self.filter_edit.text().strip().lower()

        items = [r for r in self._runs if r.job_id == job_id]
        items.sort(key=lambda r: r.started_at, reverse=True)

        self.history.clear()
        for r in items:
            text = f"{_format_dt(r.started_at)}  {r.status.upper()}  {r.run_id}"
            if needle and needle not in text.lower():
                continue

            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, r.run_id)
            if r.status == "failed":
                item.setForeground(Qt.red)
            self.history.addItem(item)

        if self.history.count() > 0:
            self.history.setCurrentRow(0)
        else:
            self.details.setPlainText("No runs found for this job (with current filter).")

    def _restore_selected(self) -> None:
        cur = self.history.currentItem()
        if cur is None:
            QMessageBox.warning(self, "Restore", "Select a run to restore.")
            return

        dest_text = self.dest.text().strip()
        if not dest_text:
            QMessageBox.warning(self, "Restore", "Choose a destination folder.")
            return

        run_id = str(cur.data(Qt.UserRole))
        run = next(r for r in self._runs if r.run_id == run_id)

        manifest_path = Path(str(run.artifact_root)) / "manifest.json"

        mode = "add-only"
        verify = "size" if self.verify_after.isChecked() else "none"
        dry_run = self.dry_run.isChecked()

        self._last_result = None
        self.details.setPlainText("Running restore…")
        self.setEnabled(False)

        self._worker.configure(
            manifest_path=manifest_path,
            destination_root=Path(dest_text),
            mode=mode,
            verify=verify,
            dry_run=dry_run,
        )
        QMetaObject.invokeMethod(self._worker, "run", Qt.ConnectionType.QueuedConnection)

    def _on_restore_finished(self, result_obj: object) -> None:
        result: RestoreRunResult = result_obj
        self._last_result = result
        self.setEnabled(True)

        self.details.setPlainText(
            "\n".join(
                [
                    "RESTORE COMPLETE",
                    f"  run_id: {result.run_id}",
                    f"  dry_run: {str(result.dry_run).lower()}",
                    f"  mode: {result.mode}",
                    f"  verify: {result.verify}",
                    f"  artifacts_root: {result.artifacts_root}",
                    f"  summary: {result.summary_path}",
                ]
            )
        )

    def _on_restore_failed(self, message: str) -> None:
        self._last_result = None
        self.setEnabled(True)
        QMessageBox.critical(self, "Restore failed", message)

    def _on_selected(self, current: QListWidgetItem | None, _prev: QListWidgetItem | None) -> None:
        if current is None:
            return
        run_id = str(current.data(Qt.UserRole))
        run = next(r for r in self._runs if r.run_id == run_id)

        lines: list[str] = []
        lines.append("RUN SUMMARY")
        lines.append(f"  run_id: {run.run_id}")
        lines.append(f"  status: {run.status}")
        lines.append(f"  dry_run: {str(run.dry_run).lower()}")
        lines.append(f"  started_at: {_format_dt(run.started_at)}")
        lines.append(f"  finished_at: {_format_dt(run.finished_at)}")
        lines.append(f"  artifact_root: {run.artifact_root}")
        lines.append("")
        lines.append("MANIFEST SUMMARY (mock)")
        for k, v in run.manifest_summary.items():
            lines.append(f"  {k}: {v}")
        lines.append("")
        lines.append("COUNTS (mock)")
        for k, v in run.counts_summary.items():
            lines.append(f"  {k}: {v}")

        self.details.setPlainText("\n".join(lines))

    def _pick_dest(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select restore destination")
        if d:
            self.dest.setText(d)

    def _mock_restore(self) -> None:
        cur = self.history.currentItem()
        if cur is None:
            QMessageBox.warning(self, "Restore", "Select a run to restore.")
            return

        dest = self.dest.text().strip()
        if not dest:
            QMessageBox.warning(self, "Restore", "Choose a destination folder.")
            return

        QMessageBox.information(
            self,
            "Mock restore",
            "This is a mock restore.\n\n"
            f"Destination: {dest}\n"
            f"Dry run: {self.dry_run.isChecked()}\n"
            f"Verify after: {self.verify_after.isChecked()}",
        )

    def closeEvent(self, event) -> None:
        self._thread.quit()
        self._thread.wait(2000)
        super().closeEvent(event)
