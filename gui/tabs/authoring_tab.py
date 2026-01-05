"""
Authoring tab for WCBT GUI.

This tab provides the user interface for editing per-job include/exclude rules.
Persistence is engine-owned and backed by the ProfileStore.

Notes
-----
- Switching jobs discards unsaved changes by design.
- Exclude rules take precedence during evaluation.
- All persistence occurs off the UI thread.
"""

from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import Qt
from PySide6.QtGui import QAction, QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QVBoxLayout,
    QWidget,
)

from gui.adapters.profile_store_adapter import GuiRuleSet, ProfileStoreAdapter
from gui.dialogs.mock_rule_editor_dialog import RuleEditorDialog
from gui.mock_data import MockJob


def _mono() -> QFont:
    f = QFont("Consolas")
    f.setStyleHint(QFont.Monospace)
    return f


@dataclass(frozen=True, slots=True)
class PatternSnapshot:
    include: list[str]
    exclude: list[str]


class AuthoringTab(QWidget):
    """
    Authoring tab for WCBT GUI.

    This tab provides the user interface for editing per-job include/exclude rules.
    Persistence is engine-owned and backed by the ProfileStore.

    Notes
    -----
    - Switching jobs discards unsaved changes by design.
    - Exclude rules take precedence during evaluation.
    - All persistence occurs off the UI thread.
    """

    def __init__(self, jobs: list[MockJob]) -> None:
        super().__init__()

        self._jobs = jobs
        self._active_job_id: str | None = None

        # Used to seed the store for unknown jobs (first time seen).
        self._default_snapshot: PatternSnapshot | None = None

        # Baseline snapshot for dirty-state tracking (last loaded/saved).
        self._baseline_snapshot: PatternSnapshot | None = None

        # Engine-backed store via a Qt adapter (no CLI calls).
        # v1 uses the default profile; we can plumb this from the app later.
        self._store = ProfileStoreAdapter(profile_name="default", data_root=None)
        self._store.rules_loaded.connect(self._on_rules_loaded)
        self._store.rules_saved.connect(self._on_rules_saved)
        self._store.unknown_job.connect(self._on_unknown_job)
        self._store.error.connect(self._on_store_error)

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)

        banner = QLabel(
            "Edits affect future backups only. Existing archives and history are not modified."
        )
        banner.setStyleSheet(
            "background:#2b2b2b; border:1px solid #555; color:#ddd; padding:6px; font-size:12px;"
        )
        root.addWidget(banner)

        # --- Top row: job selector + status + actions ---
        top = QWidget()
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 0)

        self.job_combo = QComboBox()
        for job in jobs:
            self.job_combo.addItem(job.name, job.job_id)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #777; padding-left: 6px;")

        self.dirty_label = QLabel("")
        self.dirty_label.setStyleSheet("color: #999; padding-left: 6px;")

        self.btn_save = QPushButton("Save")
        self.btn_save.setToolTip("Save rules for this job. Affects future backups only.")
        self.btn_revert = QPushButton("Revert")
        self.btn_revert.setToolTip("Revert to last saved rules for this job.")

        self.btn_rename = QPushButton("Rename…")
        self.btn_rename.setToolTip(
            "Rename this job (UI only for v1). Archives and history are not modified."
        )

        self.btn_duplicate = QPushButton("Duplicate…")
        self.btn_duplicate.setToolTip("Not implemented yet (engine-backed persistence v1).")
        self.btn_delete_job = QPushButton("Delete…")
        self.btn_delete_job.setToolTip("Not implemented yet (engine-backed persistence v1).")

        # Disable job lifecycle actions until the store supports them.
        self.btn_duplicate.setEnabled(False)
        self.btn_delete_job.setEnabled(False)

        top_layout.addWidget(QLabel("Job:"))
        top_layout.addWidget(self.job_combo)
        top_layout.addItem(QSpacerItem(20, 0, QSizePolicy.Expanding, QSizePolicy.Minimum))
        top_layout.addWidget(self.status_label)
        top_layout.addWidget(self.dirty_label)
        top_layout.addWidget(self.btn_rename)
        top_layout.addWidget(self.btn_duplicate)
        top_layout.addWidget(self.btn_delete_job)
        top_layout.addWidget(self.btn_revert)
        top_layout.addWidget(self.btn_save)

        root.addWidget(top)

        # --- Main editor row ---
        editor_row = QHBoxLayout()
        root.addLayout(editor_row, 1)

        self._include = self._build_list(
            title="Include patterns",
            items=[
                "mods/**",
                "config/**",
                "libraries/**",
                "Lighthouse_Archipelago/**",
                "*.json",
                "*.properties",
                "*.jar",
            ],
        )
        editor_row.addWidget(self._include["box"], 1)

        controls = QVBoxLayout()
        controls.setAlignment(Qt.AlignCenter)

        self.btn_to_exclude = QPushButton("→ Exclude")
        self.btn_to_exclude.setToolTip(
            "Move selected patterns to Exclude. If a path matches both include and exclude, Exclude wins (v1)."
        )
        self.btn_to_include = QPushButton("← Include")
        self.btn_to_include.setToolTip(
            "Move selected patterns to Include. If a path matches both include and exclude, Exclude wins (v1)."
        )

        self.btn_edit = QPushButton("Edit…")
        self.btn_edit.setToolTip("Advanced: edit exactly one selected rule.")
        self.btn_delete = QPushButton("Delete")
        self.btn_delete.setToolTip(
            "Advanced: remove selected rule(s). This affects future backups only."
        )

        self.btn_add = QPushButton("Add…")
        self.btn_add.setToolTip(
            "Add a rule (glob pattern). Root-relative, use / separators. Opens syntax help."
        )

        self.btn_add.clicked.connect(self._add_rule)
        self.btn_to_exclude.clicked.connect(self._move_to_exclude)
        self.btn_to_include.clicked.connect(self._move_to_include)
        self.btn_edit.clicked.connect(self._edit_selected_pattern)
        self.btn_delete.clicked.connect(self._delete_selected)

        self.btn_save.clicked.connect(self._save_current)
        self.btn_revert.clicked.connect(self._revert_current)
        self.btn_rename.clicked.connect(self._rename_job)

        controls.addWidget(self.btn_to_exclude)
        controls.addWidget(self.btn_to_include)
        controls.addWidget(QLabel(""))
        controls.addWidget(self.btn_edit)
        controls.addWidget(self.btn_add)
        controls.addWidget(self.btn_delete)

        editor_row.addLayout(controls)

        self._exclude = self._build_list(
            title="Exclude patterns",
            items=[
                "logs/**",
                "crash-reports/**",
                "__pycache__/**",
            ],
        )
        editor_row.addWidget(self._exclude["box"], 1)

        # Context menus (RMB)
        self._install_context_menu(self._include["list"])
        self._install_context_menu(self._exclude["list"])

        # Keep action enabled-state correct as selection changes.
        self._include["list"].itemSelectionChanged.connect(self._sync_action_enabled_state)
        self._exclude["list"].itemSelectionChanged.connect(self._sync_action_enabled_state)

        # Capture defaults after both lists exist.
        self._default_snapshot = self._snapshot_from_ui()

        # Job change wiring.
        self.job_combo.currentIndexChanged.connect(self._on_job_changed)

        # Initial state.
        self._sync_action_enabled_state()
        self._sync_dirty_state()

        # Kick initial load once, safely.
        try:
            self._on_job_changed()
        except Exception:
            self._store.shutdown()
            raise

        hint = QLabel(
            "Tip: Ctrl/Shift for multi-select. Right-click for advanced actions like Edit."
        )
        hint.setStyleSheet("color: #666; padding-top: 8px;")
        root.addWidget(hint)

    # ---------- UI helpers ----------
    def _build_list(self, title: str, items: list[str]) -> dict:
        box = QGroupBox(title)
        layout = QVBoxLayout(box)

        lst = QListWidget()
        lst.setSelectionMode(QAbstractItemView.ExtendedSelection)
        lst.setFont(_mono())

        for it in items:
            lst.addItem(QListWidgetItem(it))

        layout.addWidget(lst)
        return {"box": box, "list": lst}

    def _set_status(self, text: str) -> None:
        """Update the status text shown in the Authoring tab."""
        self.status_label.setText(text)

    def _install_context_menu(self, lst: QListWidget) -> None:
        lst.setContextMenuPolicy(Qt.CustomContextMenu)
        lst.customContextMenuRequested.connect(
            lambda pos, last=lst: self._show_context_menu(last, pos)
        )

    def _active_list(self) -> QListWidget:
        if self._include["list"].hasFocus():
            return self._include["list"]
        if self._exclude["list"].hasFocus():
            return self._exclude["list"]
        if self._include["list"].selectedItems():
            return self._include["list"]
        return self._exclude["list"]

    def _single_selected_item(self) -> tuple[QListWidget, QListWidgetItem] | None:
        items_inc = self._include["list"].selectedItems()
        items_exc = self._exclude["list"].selectedItems()
        total = len(items_inc) + len(items_exc)
        if total != 1:
            return None
        if items_inc:
            return self._include["list"], items_inc[0]
        return self._exclude["list"], items_exc[0]

    def _sync_action_enabled_state(self) -> None:
        is_single = self._single_selected_item() is not None
        self.btn_edit.setEnabled(is_single)

        active = self._active_list()
        self.btn_delete.setEnabled(len(active.selectedItems()) > 0)

    def _snapshot_from_ui(self) -> PatternSnapshot:
        inc = [self._include["list"].item(i).text() for i in range(self._include["list"].count())]
        exc = [self._exclude["list"].item(i).text() for i in range(self._exclude["list"].count())]
        return PatternSnapshot(include=inc, exclude=exc)

    def _apply_snapshot_to_ui(self, snap: PatternSnapshot) -> None:
        self._include["list"].clear()
        self._exclude["list"].clear()
        for p in snap.include:
            self._include["list"].addItem(QListWidgetItem(p))
        for p in snap.exclude:
            self._exclude["list"].addItem(QListWidgetItem(p))
        self._sync_action_enabled_state()

    def _sync_dirty_state(self) -> None:
        job_id = self._active_job_id
        if job_id is None or self._baseline_snapshot is None:
            self.dirty_label.setText("")
            self.btn_revert.setEnabled(False)
            self.btn_save.setEnabled(False)
            return

        current = self._snapshot_from_ui()
        is_dirty = current != self._baseline_snapshot

        self.dirty_label.setText("Unsaved changes" if is_dirty else "Saved")
        self.btn_revert.setEnabled(is_dirty)
        self.btn_save.setEnabled(is_dirty)

    # ---------- Context menu ----------
    def _show_context_menu(self, lst: QListWidget, pos) -> None:
        menu = QMenu(self)
        lst.setFocus(Qt.MouseFocusReason)

        item_under = lst.itemAt(pos)
        if item_under is not None and not item_under.isSelected():
            lst.clearSelection()
            item_under.setSelected(True)

        self._sync_action_enabled_state()
        self._sync_dirty_state()

        act_include = QAction("Include", self)
        act_exclude = QAction("Exclude", self)

        act_edit = QAction("Edit rule…", self)
        act_edit.setEnabled(self._single_selected_item() is not None)

        act_delete = QAction("Delete rule (advanced)", self)
        act_delete.setEnabled(len(lst.selectedItems()) > 0)

        act_include.triggered.connect(self._move_selected_to_include)
        act_exclude.triggered.connect(self._move_selected_to_exclude)
        act_edit.triggered.connect(self._edit_selected_pattern)
        act_delete.triggered.connect(self._delete_selected)

        menu.addAction(act_include)
        menu.addAction(act_exclude)
        menu.addSeparator()
        menu.addAction(act_edit)
        menu.addSeparator()
        menu.addAction(act_delete)

        menu.exec(lst.mapToGlobal(pos))

    # ---------- Moves ----------
    def _move_items(self, src: QListWidget, dst: QListWidget) -> None:
        for item in src.selectedItems():
            src.takeItem(src.row(item))
            dst.addItem(item)

    def _move_to_exclude(self) -> None:
        self._move_items(self._include["list"], self._exclude["list"])
        self._sync_action_enabled_state()
        self._sync_dirty_state()

    def _move_to_include(self) -> None:
        self._move_items(self._exclude["list"], self._include["list"])
        self._sync_action_enabled_state()
        self._sync_dirty_state()

    def _move_selected_to_include(self) -> None:
        if self._exclude["list"].selectedItems():
            self._move_to_include()

    def _move_selected_to_exclude(self) -> None:
        if self._include["list"].selectedItems():
            self._move_to_exclude()

    # ---------- Edit, add, delete ----------
    def _edit_selected_pattern(self) -> None:
        sel = self._single_selected_item()
        if sel is None:
            QMessageBox.information(self, "Edit rule", "Select exactly one item to edit.")
            return

        _lst, item = sel
        current = item.text()

        dlg = RuleEditorDialog(
            self,
            title="Refine Rule (Future Backups Only)",
            initial_pattern=current,
            mode_label="Rule",
        )
        if dlg.exec() == RuleEditorDialog.Accepted:
            res = dlg.result_value()
            if res is not None and res.pattern.strip():
                item.setText(res.pattern.strip())

        self._sync_action_enabled_state()
        self._sync_dirty_state()

    def _delete_selected(self) -> None:
        active = self._active_list()
        for item in active.selectedItems():
            active.takeItem(active.row(item))
        self._sync_action_enabled_state()
        self._sync_dirty_state()

    def _add_rule(self) -> None:
        active = self._active_list()
        dlg = RuleEditorDialog(self, title="Add rule", initial_pattern="", mode_label="Rule")
        if dlg.exec() == RuleEditorDialog.Accepted:
            res = dlg.result_value()
            if res is not None and res.pattern.strip():
                active.addItem(QListWidgetItem(res.pattern.strip()))
        self._sync_action_enabled_state()
        self._sync_dirty_state()

    # ---------- Store-backed job + save/revert ----------
    def _current_job_id(self) -> str:
        return str(self.job_combo.currentData())

    def _on_job_changed(self) -> None:
        new_job_id = self._current_job_id()
        self._active_job_id = new_job_id

        self._set_status("Loading…")
        self.btn_save.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self._store.request_load_rules.emit(new_job_id)

    def _save_current(self) -> None:
        job_id = self._active_job_id
        if job_id is None:
            return

        name = next((j.name for j in self._jobs if j.job_id == job_id), job_id)
        snap = self._snapshot_from_ui()
        rules = GuiRuleSet(include=tuple(snap.include), exclude=tuple(snap.exclude))

        self._set_status("Saving…")
        self.btn_save.setEnabled(False)
        self._store.request_save_rules.emit(job_id, name, rules)

    def _revert_current(self) -> None:
        job_id = self._active_job_id
        if job_id is None:
            return

        self._set_status("Reverting…")
        self.btn_save.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self._store.request_load_rules.emit(job_id)

    def _on_rules_loaded(self, job_id: str, rules_obj: object) -> None:
        if self._active_job_id != job_id:
            return

        rules = rules_obj
        assert isinstance(rules, GuiRuleSet)

        snap = PatternSnapshot(include=list(rules.include), exclude=list(rules.exclude))
        self._apply_snapshot_to_ui(snap)

        self._baseline_snapshot = snap
        self._set_status("Loaded")
        self._sync_dirty_state()

    def _on_rules_saved(self, job_id: str) -> None:
        if self._active_job_id != job_id:
            return

        self._baseline_snapshot = self._snapshot_from_ui()
        self._set_status("Saved")
        self._sync_dirty_state()

    def _on_unknown_job(self, job_id: str) -> None:
        if self._active_job_id != job_id:
            return

        default = self._default_snapshot or self._snapshot_from_ui()
        self._apply_snapshot_to_ui(default)
        self._baseline_snapshot = default

        name = next((j.name for j in self._jobs if j.job_id == job_id), job_id)
        rules = GuiRuleSet(include=tuple(default.include), exclude=tuple(default.exclude))

        self._set_status("Initializing…")
        self.btn_save.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self._store.request_save_rules.emit(job_id, name, rules)

    def _on_store_error(self, job_id: str, message: str) -> None:
        if self._active_job_id != job_id:
            return

        self._set_status("Error")
        QMessageBox.critical(self, "Profile Store Error", message)
        self._sync_dirty_state()

    # ---------- Rename (UI-only in v1) ----------
    def _job_name_exists(self, name: str) -> bool:
        name_l = name.strip().lower()
        for i in range(self.job_combo.count()):
            if str(self.job_combo.itemText(i)).strip().lower() == name_l:
                return True
        return False

    def _rename_job(self) -> None:
        current_name = str(self.job_combo.currentText()).strip()
        text, ok = QInputDialog.getText(self, "Rename job", "Job name:", text=current_name)
        if not ok:
            return

        new_name = text.strip()
        if not new_name:
            QMessageBox.warning(self, "Rename job", "Job name cannot be empty.")
            return

        if new_name.lower() != current_name.lower() and self._job_name_exists(new_name):
            QMessageBox.warning(self, "Rename job", "A job with that name already exists.")
            return

        self.job_combo.setItemText(self.job_combo.currentIndex(), new_name)

    def shutdown(self) -> None:
        """Stop background workers owned by this tab."""
        self._store.shutdown()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """Ensure the store worker thread is stopped when the widget closes."""
        try:
            self.shutdown()
        finally:
            super().closeEvent(event)
