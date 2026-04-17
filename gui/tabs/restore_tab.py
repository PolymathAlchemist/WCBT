"""
Restore tab (engine-backed).

- Lists discovered backup runs by scanning an archive root for run manifests.
- Allows the user to select a manifest.json and execute restore via engine restore service.
"""

from __future__ import annotations

import json
import shutil
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
    QVBoxLayout,
    QWidget,
)

from backup_engine.manifest_store import BackupRunSummary, list_backup_runs
from backup_engine.oz0_paths import (
    resolve_legacy_oz0_root,
    resolve_oz0_artifact_root,
    resolve_primary_oz0_root,
)
from backup_engine.profile_store.sqlite_store import open_profile_store
from backup_engine.restore.service import RestoreRunResult, run_restore
from gui.adapters.profile_store_adapter import ProfileStoreAdapter
from gui.settings_store import GuiSettings, load_gui_settings, save_gui_settings

_ALL_HISTORY_LABEL = "All history"


def _format_mtime(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _backup_origin_display_label(backup_origin: str | None) -> str | None:
    """
    Return a human-readable label for a manifest backup origin.

    Parameters
    ----------
    backup_origin : str | None
        Raw manifest backup origin value.

    Returns
    -------
    str | None
        User-facing label for a recognized origin, otherwise ``None``.
    """
    labels = {
        "normal": "Normal backup",
        "scheduled": "Scheduled backup",
        "pre_restore": "Pre-restore safeguard backup",
    }
    return labels.get(backup_origin)


def _history_backup_origin_suffix(backup_origin: str | None) -> str:
    """
    Build the compact history-row suffix for non-default backup origins.

    Parameters
    ----------
    backup_origin : str | None
        Raw manifest backup origin value.

    Returns
    -------
    str
        Compact suffix appended to history row text for recognized origins.
    """
    origin_label = _backup_origin_display_label(backup_origin)
    if origin_label is None:
        return ""
    return f"  [{origin_label}]"


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
        backup_origin_label = _backup_origin_display_label(
            data.get("backup_origin") if isinstance(data.get("backup_origin"), str) else None
        )
        if backup_origin_label is not None:
            summary["backup_origin"] = backup_origin_label
        backup_note = data.get("backup_note")
        if isinstance(backup_note, str) and backup_note.strip():
            summary["backup_note"] = backup_note.strip()
        summary["top_level_keys"] = sorted(list(data.keys()))[:25]
        return summary

    return {"type": type(data).__name__}


class RestoreWorker(QObject):
    """
    Background worker that executes a restore run off the UI thread.

    Responsibilities
    ----------------
    - Invoke the engine restore service using the configured manifest and destination.
    - Emit completion or failure signals back to the GUI.
    """

    finished = Signal(object)  # RestoreRunResult
    failed = Signal(str)

    def __init__(self, *, data_root: Path | None = None) -> None:
        super().__init__()
        self._manifest_path: Path | None = None
        self._destination_root: Path | None = None
        self._mode: str = "add-only"
        self._verify: str = "size"
        self._dry_run: bool = True
        self._data_root = data_root
        self._pre_restore_backup_compression: str = "zip"

    def configure(
        self,
        *,
        manifest_path: Path,
        destination_root: Path,
        mode: str,
        verify: str,
        dry_run: bool,
        pre_restore_backup_compression: str,
    ) -> None:
        """
        Configure the next restore execution parameters.

        Notes
        -----
        This method does not perform any I/O. It stores parameters for the next `run()` call.
        """
        self._manifest_path = manifest_path
        self._destination_root = destination_root
        self._mode = mode
        self._verify = verify
        self._dry_run = dry_run
        self._pre_restore_backup_compression = pre_restore_backup_compression

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
                data_root=self._data_root,
                pre_restore_backup_compression=self._pre_restore_backup_compression,
            )
            self.finished.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class RestoreTab(QWidget):
    """
    Restore tab for the WCBT GUI.

    Responsibilities
    ----------------
    - List backup runs discovered from run manifests under the selected archive root.
    - Persist per-job restore defaults (archive root, destination root) via ProfileStore.
    - Execute restores on a worker thread and present result artifacts to the user.

    Notes
    -----
    - The tab does not call the CLI. All operations are engine-backed.
    - Manifest discovery is read-only and does not modify archives.
    """

    def __init__(self) -> None:
        super().__init__()

        self._settings = load_gui_settings(data_root=None)
        self._loading_settings = True
        self._known_job_ids: list[str] = []
        self._store = ProfileStoreAdapter(
            profile_name="default",
            data_root=self._settings.data_root,
        )
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
        self.filter_edit.setPlaceholderText("Filter runs (status, run id, note, etc.)")
        self.filter_edit.textChanged.connect(self._refresh_history)

        top_layout.addWidget(QLabel("Job:"))
        top_layout.addWidget(self.job_combo)
        top_layout.addSpacing(10)
        top_layout.addWidget(self.filter_edit, 1)
        outer.addWidget(top)

        archive = QWidget()
        archive_layout = QHBoxLayout(archive)
        archive_layout.setContentsMargins(0, 0, 0, 0)

        self.archive_root_label = QLabel("Artifacts root:")
        self.archive_root = QLineEdit()
        self.archive_root.setPlaceholderText("Select backup archive root to scan for manifests…")
        self.archive_root.textChanged.connect(self._on_archive_root_changed)
        self.archive_root.setPlaceholderText("Authoritative job artifacts root or history override")

        btn_pick_archive = QPushButton("Browse…")
        btn_pick_archive.clicked.connect(self._pick_archive_root)

        archive_layout.addWidget(self.archive_root_label)
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

        self.restore_resolution = QPlainTextEdit()
        self.restore_resolution.setReadOnly(True)
        self.restore_resolution.setFont(_mono())
        self.restore_resolution.setPlaceholderText("Restore input resolution will appear here.")
        right_layout.addWidget(self.restore_resolution, 1)

        links = QGroupBox("Artifacts")
        links_layout = QHBoxLayout(links)

        self.btn_open_manifest_folder = QPushButton("Open manifest folder")
        self.btn_open_manifest_folder.clicked.connect(self._open_manifest_folder)

        self.btn_open_artifacts_root = QPushButton("Open artifacts root")
        self.btn_open_artifacts_root.clicked.connect(self._open_artifacts_root)

        self.btn_housekeeping = QPushButton("Housekeeping...")
        self.btn_housekeeping.clicked.connect(self._run_housekeeping)

        links_layout.addWidget(self.btn_open_manifest_folder)
        links_layout.addWidget(self.btn_open_artifacts_root)
        links_layout.addWidget(self.btn_housekeeping)
        right_layout.addWidget(links)

        restore_box = QGroupBox("Restore")
        self.restore_layout = QFormLayout(restore_box)

        self.dest = QLineEdit()
        self.dest.textChanged.connect(self._on_dest_changed)
        btn_browse = QPushButton("Browse…")
        btn_browse.clicked.connect(self._pick_dest)

        dest_row = QWidget()
        dest_row_layout = QHBoxLayout(dest_row)
        dest_row_layout.setContentsMargins(0, 0, 0, 0)
        dest_row_layout.addWidget(self.dest, 1)
        dest_row_layout.addWidget(btn_browse)
        self.restore_layout.addRow("Destination:", dest_row)
        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Add-only (safe)", "add-only")
        self.mode_combo.addItem("Overwrite", "overwrite")
        self.restore_layout.addRow("Mode:", self.mode_combo)

        self.dry_run = QCheckBox("Dry run")

        self.verify_combo = QComboBox()
        self.verify_combo.addItem("None", "none")
        self.verify_combo.addItem("Size", "size")
        self._apply_restore_preferences()
        self.mode_combo.currentIndexChanged.connect(self._on_restore_preferences_changed)
        self.verify_combo.currentIndexChanged.connect(self._on_restore_preferences_changed)
        self.dry_run.checkStateChanged.connect(self._on_restore_preferences_changed)
        self._loading_settings = False

        opts_row = QWidget()
        opts_layout = QHBoxLayout(opts_row)
        opts_layout.setContentsMargins(0, 0, 0, 0)
        opts_layout.addWidget(self.dry_run)
        opts_layout.addSpacing(12)
        opts_layout.addWidget(QLabel("Verify:"))
        opts_layout.addWidget(self.verify_combo)
        opts_layout.addStretch(1)
        self.restore_layout.addRow("Options:", opts_row)

        self.btn_restore = QPushButton("Restore Selected Run")
        self.btn_restore.clicked.connect(self._restore_selected)
        self.restore_layout.addRow("", self.btn_restore)

        self.btn_copy_summary = QPushButton("Copy Restore Summary")
        self.btn_copy_summary.clicked.connect(self._copy_restore_summary)
        self.restore_layout.addRow("", self.btn_copy_summary)

        right_layout.addWidget(restore_box, 1)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([360, 820])

        outer.addWidget(splitter, 1)

        self._refresh_history()

        self._last_result: RestoreRunResult | None = None

        self._thread = QThread(self)
        self._worker = RestoreWorker(data_root=self._settings.data_root)
        self._worker.moveToThread(self._thread)
        self._worker.finished.connect(self._on_restore_finished)
        self._worker.failed.connect(self._on_restore_failed)
        self._thread.start()

        self._store.request_list_jobs.emit()

    def _compute_restore_resolution(self, manifest_path: Path) -> list[str]:
        lines: list[str] = []
        lines.append("RESTORE INPUT RESOLUTION")

        lines.append("  input: manifest.json")

        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            lines.append("  status: unreadable manifest")
            return lines

        archive = payload.get("archive")
        if not isinstance(archive, dict):
            lines.append("  archive metadata: absent")
            lines.append("  restore source: run directory")
            return lines

        rel = archive.get("relative_path")
        lines.append("  archive metadata: present")

        if not isinstance(rel, str) or not rel.strip():
            lines.append("  archive path: invalid")
            lines.append("  restore source: run directory")
            return lines

        archive_path = (manifest_path.parent / rel).resolve()
        lines.append(f"  archive path: {archive_path}")

        if archive_path.exists() and archive_path.is_file():
            lines.append("  archive exists: yes")
            lines.append("  restore source: derived archive")
        else:
            lines.append("  archive exists: no")
            lines.append("  restore source: run directory")

        return lines

    def _copy_restore_summary(self) -> None:
        text = self._build_restore_summary()
        QApplication.clipboard().setText(text)
        QMessageBox.information(self, "Restore Summary", "Copied restore summary to clipboard.")

    def _selected_job_id(self) -> str | None:
        current_job_id = self.job_combo.currentData()
        if current_job_id is None:
            return None
        return str(current_job_id)

    def _on_job_changed(self) -> None:
        job_id = self._selected_job_id()
        authoritative_display_text = self._resolve_archive_root_display_text(None)
        if authoritative_display_text:
            self.archive_root.blockSignals(True)
            try:
                self.archive_root.setText(authoritative_display_text)
            finally:
                self.archive_root.blockSignals(False)
        self._update_archive_root_field_presentation()
        if job_id is not None:
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
            self.job_combo.addItem(_ALL_HISTORY_LABEL, None)
            self._known_job_ids = []
            for js in jobs:
                job_id = str(getattr(js, "job_id"))
                name = str(getattr(js, "name"))
                self._known_job_ids.append(job_id)
                self.job_combo.addItem(name, job_id)
            self.job_combo.setCurrentIndex(1 if len(jobs) > 0 else 0)
        finally:
            self.job_combo.blockSignals(False)

        self.job_combo.setEnabled(True)
        if len(jobs) == 0:
            self.details.setPlainText(
                "No active jobs. Choose an archive root to browse restore history."
            )
        self._on_job_changed()

    def _on_store_error(self, job_id: str, message: str) -> None:
        QMessageBox.critical(self, "Profile Store Error", message)

    def _on_restore_defaults_loaded(self, job_id: str, payload: object) -> None:
        # Only apply if the currently selected job matches.
        selected_job_id = self._selected_job_id()
        if selected_job_id is None or job_id != selected_job_id:
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
            self.archive_root.setText(self._resolve_archive_root_display_text(archive_root))
            if restore_dest_root is not None:
                self.dest.setText(restore_dest_root)
        finally:
            self.archive_root.blockSignals(False)
            self.dest.blockSignals(False)

        self._update_archive_root_field_presentation()
        self._refresh_history()

    def _on_restore_defaults_saved(self, job_id: str) -> None:
        # No UI action needed; this is here for future status indicators.
        _ = job_id

    def _on_archive_root_changed(self) -> None:
        self._update_archive_root_field_presentation()
        job_id = self._selected_job_id()
        if job_id is not None:
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
        if job_id is None:
            return
        self._store.request_save_restore_defaults.emit(
            job_id,
            {
                "archive_root": self.archive_root.text().strip() or None,
                "restore_dest_root": self.dest.text().strip() or None,
            },
        )

    def _apply_restore_preferences(self) -> None:
        """
        Apply persisted Restore tab preferences to the current widgets.
        """
        self._select_combo_by_data(self.mode_combo, self._settings.restore_mode)
        self._select_combo_by_data(self.verify_combo, self._settings.restore_verify)
        self.dry_run.setChecked(self._settings.restore_dry_run)

    @staticmethod
    def _select_combo_by_data(combo: QComboBox, value: str) -> None:
        for index in range(combo.count()):
            if str(combo.itemData(index)) == value:
                combo.setCurrentIndex(index)
                return

    def _on_restore_preferences_changed(self, *_args: object) -> None:
        """
        Persist the active Restore tab behavior preferences.
        """
        if getattr(self, "_loading_settings", False):
            return

        updated_settings = GuiSettings(
            data_root=self._settings.data_root,
            archives_root=self._settings.archives_root,
            default_compression=self._settings.default_compression,
            default_run_mode=self._settings.default_run_mode,
            restore_mode=str(self.mode_combo.currentData()),
            restore_verify=str(self.verify_combo.currentData()),
            restore_dry_run=self.dry_run.isChecked(),
            pre_restore_backup_compression=self._settings.pre_restore_backup_compression,
        )
        save_gui_settings(data_root=None, settings=updated_settings)
        self._settings = updated_settings

    def _open_store(self):
        """
        Open the profile store using the persisted GUI settings context.
        """
        return open_profile_store(profile_name="default", data_root=self._settings.data_root)

    def _load_selected_job_binding_source_root(self) -> Path | None:
        """
        Return the selected job's persisted source root, if available.

        Returns
        -------
        Path | None
            Source root for the selected job, or ``None`` when unavailable.
        """
        selected_job_id = self._selected_job_id()
        if selected_job_id is None:
            return None

        try:
            binding = self._open_store().load_job_binding(selected_job_id)
        except Exception:
            return None

        if not binding.source_root.strip():
            return None
        return Path(binding.source_root)

    def _resolve_authoritative_job_artifacts_root(self) -> Path | None:
        """
        Return the selected job's authoritative artifacts root.

        Returns
        -------
        Path | None
            Target-partitioned OZ0 root derived from the selected job binding,
            or ``None`` when the selected job has no resolvable source root.
        """
        source_root = self._load_selected_job_binding_source_root()
        if source_root is None:
            return None
        return resolve_oz0_artifact_root(source_root)

    def _resolve_archive_root_display_text(self, archive_root: str | None) -> str:
        """
        Resolve the archive-root field text for the selected job.

        Parameters
        ----------
        archive_root : str | None
            Persisted restore default or manual override candidate.

        Returns
        -------
        str
            Display text for the archive-root field.

        Notes
        -----
        Older sessions could persist the legacy shared ``OZ0`` path in this
        field. When the selected job now resolves to a target-partitioned OZ0
        root, that legacy value is stale and should no longer be presented as if
        it were authoritative.
        """
        authoritative_root = self._resolve_authoritative_job_artifacts_root()
        if authoritative_root is None:
            return archive_root or ""

        if archive_root is None or not archive_root.strip():
            return str(authoritative_root)

        displayed_root = Path(archive_root)
        if displayed_root == authoritative_root:
            return str(authoritative_root)

        source_root = self._load_selected_job_binding_source_root()
        if source_root is not None:
            legacy_root = resolve_legacy_oz0_root(source_root)
            if displayed_root == legacy_root and legacy_root != authoritative_root:
                return str(authoritative_root)

        return archive_root

    def _update_archive_root_field_presentation(self) -> None:
        """
        Update the archive-root field label and placeholder for current semantics.
        """
        authoritative_root = self._resolve_authoritative_job_artifacts_root()
        archive_text = self.archive_root.text().strip()

        if authoritative_root is not None and archive_text:
            if Path(archive_text) == authoritative_root:
                self.archive_root_label.setText("Artifacts root:")
                self.archive_root.setPlaceholderText(
                    "Using the selected job's authoritative artifacts root"
                )
                return

        self.archive_root_label.setText("History root override:")
        if authoritative_root is not None:
            self.archive_root.setPlaceholderText(
                f"Override history scan root (authoritative: {authoritative_root})"
            )
        else:
            self.archive_root.setPlaceholderText("Optional manual history scan root override")

    def _derive_history_roots_from_jobs(self, selected_job_id: str | None) -> list[Path]:
        """
        Return OZ0 roots derived from live job bindings.

        Parameters
        ----------
        selected_job_id : str | None
            Selected job identifier, or ``None`` to derive roots for all known jobs.

        Returns
        -------
        list[Path]
            Unique OZ0 roots derived from persisted job bindings, preferring the
            target-partitioned layout before the legacy shared root.
        """
        target_job_ids = [selected_job_id] if selected_job_id is not None else self._known_job_ids
        if not target_job_ids:
            return []

        roots_by_key: dict[str, Path] = {}
        store = self._open_store()
        for job_id in target_job_ids:
            try:
                binding = store.load_job_binding(job_id)
            except Exception:
                continue
            if not binding.source_root.strip():
                continue
            source_root = Path(binding.source_root)
            for root in (
                resolve_primary_oz0_root(source_root),
                resolve_legacy_oz0_root(source_root),
            ):
                roots_by_key[str(root)] = root
        return list(roots_by_key.values())

    def _resolve_history_roots(self) -> list[Path]:
        """
        Return the artifact roots that should be scanned for restore history.

        Returns
        -------
        list[Path]
            Root directories to scan with manifest discovery.
        """
        roots_by_key: dict[str, Path] = {}
        archive_text = self.archive_root.text().strip()
        if archive_text:
            manual_root = Path(archive_text)
            roots_by_key[str(manual_root)] = manual_root

        for derived_root in self._derive_history_roots_from_jobs(self._selected_job_id()):
            roots_by_key[str(derived_root)] = derived_root
        return list(roots_by_key.values())

    def _resolve_authoritative_artifacts_root(self) -> Path | None:
        """
        Resolve the authoritative artifacts root for the current Restore tab context.

        Returns
        -------
        Path | None
            Selected job's authoritative OZ0 root when available, otherwise the
            manually entered archive root used for history discovery. Returns
            ``None`` when neither is available.
        """
        authoritative_root = self._resolve_authoritative_job_artifacts_root()
        if authoritative_root is not None:
            return authoritative_root

        archive_text = self.archive_root.text().strip()
        if archive_text:
            return Path(archive_text)
        return None

    def _housekeeping_base_roots(self) -> list[Path]:
        """
        Return roots whose sibling transient restore residue may be cleaned.

        Returns
        -------
        list[Path]
            Unique base roots used to derive safe transient cleanup targets.
        """
        roots_by_key: dict[str, Path] = {}

        destination_text = self.dest.text().strip()
        if destination_text:
            destination_root = Path(destination_text)
            roots_by_key[str(destination_root)] = destination_root

        source_root = self._load_selected_job_binding_source_root()
        if source_root is not None:
            roots_by_key[str(source_root)] = source_root

        return list(roots_by_key.values())

    def _collect_housekeeping_targets(self) -> list[Path]:
        """
        Collect safe restore residue paths for manual cleanup.

        Returns
        -------
        list[Path]
            Existing restore residue directories safe to delete.
        """
        targets_by_key: dict[str, Path] = {}
        for base_root in self._housekeeping_base_roots():
            candidates = [
                base_root.with_name(f"{base_root.name}.wcbt_stage"),
                base_root.with_name(f"{base_root.name}.wcbt_restore_extract"),
            ]
            candidates.extend(
                candidate
                for candidate in base_root.parent.glob(f".wcbt_restore_previous_{base_root.name}_*")
            )
            for candidate in candidates:
                if candidate.exists() and candidate.is_dir():
                    targets_by_key[str(candidate)] = candidate
        return list(targets_by_key.values())

    def _refresh_history(self) -> None:
        needle = self.filter_edit.text().strip().lower()
        history_roots = self._resolve_history_roots()

        self.history.clear()
        self._selected_manifest_path = None

        if hasattr(self, "btn_restore"):
            self.btn_restore.setEnabled(False)
        if hasattr(self, "restore_resolution"):
            self.restore_resolution.setPlainText("")

        if not history_roots:
            self.details.setPlainText(
                "Choose an archive root or select a job with a saved source root to scan "
                "authoritative OZ0 manifests, including historical runs."
            )
            return

        invalid_roots = [root for root in history_roots if not root.exists() or not root.is_dir()]
        if len(invalid_roots) == len(history_roots):
            self.details.setPlainText("Archive root does not exist.")
            return

        selected_job_id = self._selected_job_id()
        runs_by_manifest_path: dict[Path, BackupRunSummary] = {}
        for root in history_roots:
            if not root.exists() or not root.is_dir():
                continue
            for run in list_backup_runs(root, limit=500):
                runs_by_manifest_path[run.manifest_path] = run

        runs = sorted(
            runs_by_manifest_path.values(),
            key=lambda run: getattr(run, "modified_at_utc"),
            reverse=True,
        )

        for r in runs:
            if selected_job_id is not None and r.job_id is not None and r.job_id != selected_job_id:
                continue
            text = (
                f"{r.modified_at_utc}  {r.run_id}  {r.manifest_path}"
                f"{_history_backup_origin_suffix(r.backup_origin)}"
            )
            searchable_parts = [
                text,
                r.profile_name or "",
                r.source_root or "",
                r.backup_origin or "",
                r.backup_note or "",
                r.job_id or "",
                r.job_name or "",
            ]
            searchable_text = " ".join(searchable_parts).lower()
            if needle and needle not in searchable_text:
                continue

            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, str(r.manifest_path))
            self.history.addItem(item)

        if self.history.count() > 0:
            self.history.setCurrentRow(0)
        else:
            self.details.setPlainText("No manifests found (with current filter).")

    def refresh_on_activate(self) -> None:
        """
        Refresh restore history when the Restore tab becomes active.
        """
        self._refresh_history()

    def _build_restore_summary(self) -> str:
        manifest_path = self._selected_manifest_path
        dest_text = self.dest.text().strip()

        lines: list[str] = []
        lines.append("WCBT RESTORE SUMMARY")

        if manifest_path is None:
            lines.append("selected_manifest: (none)")
            return "\n".join(lines)

        lines.append(f"selected_manifest: {manifest_path}")
        lines.append(f"destination: {dest_text or '(empty)'}")
        lines.append(f"mode: {str(self.mode_combo.currentData())}")
        lines.append(f"verify: {str(self.verify_combo.currentData())}")
        lines.append(f"dry_run: {str(self.dry_run.isChecked()).lower()}")
        manifest_summary = _safe_read_manifest_summary(manifest_path)
        backup_origin_value = manifest_summary.get("backup_origin")
        if isinstance(backup_origin_value, str):
            lines.append(f"backup_origin: {backup_origin_value}")
        backup_note_value = manifest_summary.get("backup_note")
        if isinstance(backup_note_value, str):
            lines.append(f"backup_note: {backup_note_value}")

        # Reuse existing resolution logic for consistency.
        lines.append("")
        lines.extend(self._compute_restore_resolution(manifest_path))

        allowed, reason = self._restore_preflight(manifest_path)
        lines.append("")
        lines.append(f"restore_enabled: {str(allowed).lower()}")
        if reason:
            lines.append(f"blocked_reason: {reason}")

        return "\n".join(lines)

    def _restore_preflight(self, manifest_path: Path) -> tuple[bool, str | None]:
        """
        Determine whether Restore should be enabled for the selected manifest.

        Returns
        -------
        tuple[bool, str | None]
            (is_allowed, reason_if_disabled)
        """
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return False, "Manifest is unreadable."

        archive = payload.get("archive")
        if not isinstance(archive, dict):
            return True, None

        rel = archive.get("relative_path")
        if not isinstance(rel, str) or not rel.strip():
            # Archive metadata is malformed; safest is to allow restore from run dir.
            return True, None

        archive_path = (manifest_path.parent / rel).resolve()
        if not (archive_path.exists() and archive_path.is_file()):
            return True, None

        return True, None

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

        mode = str(self.mode_combo.currentData())
        verify = str(self.verify_combo.currentData())
        dry_run = self.dry_run.isChecked()

        self._last_result = None
        self.details.setPlainText(f"Running restore…\n\nmode: {mode}")
        self.setEnabled(False)

        self._worker.configure(
            manifest_path=manifest_path,
            destination_root=Path(dest_text),
            mode=mode,
            verify=verify,
            dry_run=dry_run,
            pre_restore_backup_compression=self._settings.pre_restore_backup_compression,
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
        backup_origin_label = None
        backup_origin_value = summary.get("backup_origin") if summary else None
        if isinstance(backup_origin_value, str):
            backup_origin_label = backup_origin_value
        if backup_origin_label is not None:
            lines.append(f"  backup_origin: {backup_origin_label}")
        backup_note_value = summary.get("backup_note") if summary else None
        if isinstance(backup_note_value, str):
            lines.append(f"  backup_note: {backup_note_value}")
        lines.append(f"  size_bytes: {size}")
        if summary:
            lines.append("")
            lines.append("SUMMARY")
            for k, v in summary.items():
                lines.append(f"  {k}: {v}")

        self.details.setPlainText("\n".join(lines))

        resolution_lines = self._compute_restore_resolution(manifest_path)
        self.restore_resolution.setPlainText("\n".join(resolution_lines))

        allowed, reason = self._restore_preflight(manifest_path)
        self.btn_restore.setEnabled(allowed)

        if not allowed and reason:
            self.restore_resolution.appendPlainText("")
            self.restore_resolution.appendPlainText("BLOCKED")
            self.restore_resolution.appendPlainText(f"  reason: {reason}")

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
        root = self._resolve_authoritative_artifacts_root()
        if root is None:
            QMessageBox.information(
                self,
                "Artifacts",
                "Choose an archive root or select a job with a saved source root first.",
            )
            return
        root.mkdir(parents=True, exist_ok=True)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(root)))

    def _run_housekeeping(self) -> None:
        """
        Delete known-safe restore residue after user confirmation.
        """
        targets = self._collect_housekeeping_targets()
        if not targets:
            QMessageBox.information(
                self,
                "Housekeeping",
                "No restore residue was found for the current destination or job.",
            )
            return

        details = "\n".join(f"  - {target}" for target in targets)
        answer = QMessageBox.question(
            self,
            "Housekeeping",
            "Delete the following restore residue?\n\n"
            f"{details}\n\n"
            "This removes only known-safe temporary restore folders and preserved pre-restore safety snapshots.",
        )
        if answer != QMessageBox.StandardButton.Yes:
            return

        for target in targets:
            shutil.rmtree(target, ignore_errors=False)

        QMessageBox.information(
            self,
            "Housekeeping",
            f"Removed {len(targets)} restore residue folder(s).",
        )

    def _pick_archive_root(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Select archive root")
        if d:
            self.archive_root.setText(d)

    def _on_restore_failed(self, message: str) -> None:
        self.setEnabled(True)
        self.details.setPlainText(f"Restore failed:\n\n{message}")
        QMessageBox.critical(self, "Restore Failed", message)

    def shutdown(self) -> None:
        """
        Shut down background workers and the ProfileStore adapter.

        Notes
        -----
        This is intended to be safe to call multiple times.
        """
        self._thread.quit()
        self._thread.wait(2000)
        self._store.shutdown()

    def closeEvent(self, event) -> None:
        self.shutdown()
        super().closeEvent(event)
