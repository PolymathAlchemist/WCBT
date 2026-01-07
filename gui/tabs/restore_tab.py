"""
Restore tab (engine-backed).

- Lists discovered backup runs by scanning an archive root for run manifests.
- Allows the user to select a manifest.json and execute restore via engine restore service.
"""

from __future__ import annotations

import json
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

from backup_engine.manifest_store import list_backup_runs
from backup_engine.restore.service import RestoreRunResult, run_restore
from gui.adapters.profile_store_adapter import ProfileStoreAdapter


def _format_mtime(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


def _safe_read_manifest_summary(manifest_path: Path) -> dict[str, object]:
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    # Keep the UI stable: only show a small top-level summary.
    if isinstance(data, dict):
        summary: dict[str, object] = {}
        for k in ["run_id", "created_at", "source_root", "profile_name", "version"]:
            if k in data:
                summary[k] = data[k]
        summary["top_level_keys"] = sorted(list(data.keys()))[:25]
        return summary

    return {"type": type(data).__name__}


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
    def __init__(self) -> None:
        super().__init__()

        self._store = ProfileStoreAdapter(profile_name="default", data_root=None)
        self._store.jobs_loaded.connect(self._on_jobs_loaded)
        self._store.error.connect(self._on_store_error)
        self._store.restore_defaults_loaded.connect(self._on_restore_defaults_loaded)
        self._store.restore_defaults_saved.connect(self._on_restore_defaults_saved)

        self._selected_manifest_path: Path | None = None

        outer = QVBoxLayout(self)
        outer.setContentsMargins(12, 12, 12, 12)

        top = QWidget()
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 0)

        self.job_combo = QComboBox()
        self.job_combo.setEnabled(False)
        self.job_combo.currentIndexChanged.connect(self._on_job_changed)

        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Filter runs (status, run id, etc.)")
        self.filter_edit.textChanged.connect(self._refresh_history)

        top_layout.addWidget(QLabel("Job:"))
        top_layout.addWidget(self.job_combo)
        top_layout.addSpacing(10)
        top_layout.addWidget(self.filter_edit, 1)
        outer.addWidget(top)

        archive = QWidget()
        archive_layout = QHBoxLayout(archive)
        archive_layout.setContentsMargins(0, 0, 0, 0)

        self.archive_root = QLineEdit()
        self.archive_root.setPlaceholderText("Select backup archive root to scan for manifests…")
        self.archive_root.textChanged.connect(self._on_archive_root_changed)

        btn_pick_archive = QPushButton("Browse…")
        btn_pick_archive.clicked.connect(self._pick_archive_root)

        archive_layout.addWidget(QLabel("Archive root:"))
        archive_layout.addWidget(self.archive_root, 1)
        archive_layout.addWidget(btn_pick_archive)
        outer.addWidget(archive)

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

        links = QGroupBox("Artifacts")
        links_layout = QHBoxLayout(links)

        self.btn_open_manifest_folder = QPushButton("Open manifest folder")
        self.btn_open_manifest_folder.clicked.connect(self._open_manifest_folder)

        self.btn_open_artifacts_root = QPushButton("Open artifacts root")
        self.btn_open_artifacts_root.clicked.connect(self._open_artifacts_root)

        links_layout.addWidget(self.btn_open_manifest_folder)
        links_layout.addWidget(self.btn_open_artifacts_root)
        right_layout.addWidget(links)

        restore_box = QGroupBox("Restore")
        restore_layout = QFormLayout(restore_box)

        self.dest = QLineEdit()
        self.dest.textChanged.connect(self._on_dest_changed)
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

        self._store.request_list_jobs.emit()

    def _selected_job_id(self) -> str:
        return str(self.job_combo.currentData())

    def _on_job_changed(self) -> None:
        job_id = self._selected_job_id()
        if job_id:
            self._store.request_load_restore_defaults.emit(job_id)
        self._refresh_history()

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
            self.details.setPlainText("No jobs yet. Create one in Authoring.")
            return

        self.job_combo.setEnabled(True)
        self._on_job_changed()

    def _on_store_error(self, job_id: str, message: str) -> None:
        QMessageBox.critical(self, "Profile Store Error", message)

    def _on_restore_defaults_loaded(self, job_id: str, payload: object) -> None:
        # Only apply if the currently selected job matches.
        if job_id != self._selected_job_id():
            return

        try:
            data = payload
            assert isinstance(data, dict)
            archive_root = data.get("archive_root")
            restore_dest_root = data.get("restore_dest_root")
            assert archive_root is None or isinstance(archive_root, str)
            assert restore_dest_root is None or isinstance(restore_dest_root, str)
        except Exception:
            return

        self.archive_root.blockSignals(True)
        self.dest.blockSignals(True)
        try:
            if archive_root is not None:
                self.archive_root.setText(archive_root)
            if restore_dest_root is not None:
                self.dest.setText(restore_dest_root)
        finally:
            self.archive_root.blockSignals(False)
            self.dest.blockSignals(False)

    def _on_restore_defaults_saved(self, job_id: str) -> None:
        # No UI action needed; this is here for future status indicators.
        _ = job_id

    def _on_archive_root_changed(self) -> None:
        job_id = self._selected_job_id()
        if job_id:
            self._store.request_save_restore_defaults.emit(
                job_id,
                {
                    "archive_root": self.archive_root.text().strip() or None,
                    "restore_dest_root": self.dest.text().strip() or None,
                },
            )
        self._refresh_history()

    def _on_dest_changed(self) -> None:
        job_id = self._selected_job_id()
        if not job_id:
            return
        self._store.request_save_restore_defaults.emit(
            job_id,
            {
                "archive_root": self.archive_root.text().strip() or None,
                "restore_dest_root": self.dest.text().strip() or None,
            },
        )

    def _refresh_history(self) -> None:
        needle = self.filter_edit.text().strip().lower()
        archive_text = self.archive_root.text().strip()

        self.history.clear()
        self._selected_manifest_path = None

        if self.job_combo.count() == 0:
            self.details.setPlainText("No jobs yet. Create one in Authoring.")
            return

        if not archive_text:
            self.details.setPlainText("Choose an archive root to scan for manifests.")
            return

        root = Path(archive_text)
        if not root.exists() or not root.is_dir():
            self.details.setPlainText("Archive root does not exist.")
            return

        selected_profile_name = str(self.job_combo.currentText()).strip() or None
        selected_profile_name = str(self.job_combo.currentText()).strip() or None
        runs = list_backup_runs(root, profile_name=selected_profile_name, limit=500)

        for r in runs:
            text = f"{r.modified_at_utc}  {r.run_id}  {r.manifest_path}"
            if needle and needle not in text.lower():
                continue

            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, str(r.manifest_path))
            self.history.addItem(item)

        if self.history.count() > 0:
            self.history.setCurrentRow(0)
        else:
            self.details.setPlainText("No manifests found (with current filter).")

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

    def _restore_selected(self) -> None:
        cur = self.history.currentItem()
        if cur is None:
            QMessageBox.warning(self, "Restore", "Select a manifest to restore.")
            return

        dest_text = self.dest.text().strip()
        if not dest_text:
            QMessageBox.warning(self, "Restore", "Choose a destination folder.")
            return

        manifest_path = Path(str(cur.data(Qt.UserRole)))
        if not manifest_path.exists():
            QMessageBox.critical(self, "Restore", "Selected manifest does not exist.")
            return

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

    def _on_selected(self, current: QListWidgetItem | None, _prev: QListWidgetItem | None) -> None:
        if current is None:
            return

        manifest_path = Path(str(current.data(Qt.UserRole)))
        self._selected_manifest_path = manifest_path

        try:
            st = manifest_path.stat()
            size = st.st_size
            mtime = st.st_mtime
        except Exception:
            size = 0
            mtime = 0.0

        summary = _safe_read_manifest_summary(manifest_path)

        lines: list[str] = []
        lines.append("MANIFEST")
        lines.append(f"  path: {manifest_path}")
        lines.append(f"  folder: {manifest_path.parent}")
        lines.append(f"  modified: {_format_mtime(mtime)}")

        lines.append(f"  size_bytes: {size}")
        if summary:
            lines.append("")
            lines.append("SUMMARY")
            for k, v in summary.items():
                lines.append(f"  {k}: {v}")

        self.details.setPlainText("\n".join(lines))

    def _pick_dest(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select restore destination")
        if d:
            self.dest.setText(d)

    def _open_manifest_folder(self) -> None:
        if self._selected_manifest_path is None:
            QMessageBox.information(self, "Artifacts", "Select a manifest first.")
            return
        folder = self._selected_manifest_path.parent
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(folder)))

    def _open_artifacts_root(self) -> None:
        if self._last_result is None:
            QMessageBox.information(self, "Artifacts", "Run a restore first.")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(self._last_result.artifacts_root)))

    def _pick_archive_root(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select archive root")
        if d:
            self.archive_root.setText(d)

    def _on_restore_failed(self, message: str) -> None:
        self.setEnabled(True)
        self.details.setPlainText(f"Restore failed:\n\n{message}")
        QMessageBox.critical(self, "Restore Failed", message)

    def shutdown(self) -> None:
        self._thread.quit()
        self._thread.wait(2000)
        self._store.shutdown()

    def closeEvent(self, event) -> None:
        self.shutdown()
        super().closeEvent(event)
