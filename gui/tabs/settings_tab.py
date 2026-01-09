from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from gui.settings_store import GuiSettings, load_gui_settings, save_gui_settings


class SettingsTab(QWidget):
    """
    Settings tab for WCBT GUI.

    Responsibilities
    ----------------
    - Configure GUI defaults (data_root, archives root, default run mode, default compression).
    - Persist settings to disk in a small JSON file under data_root.
    """

    def __init__(self) -> None:
        super().__init__()

        self._settings = load_gui_settings(data_root=None)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)

        box = QGroupBox("Defaults")
        box_layout = QVBoxLayout(box)

        # Data root
        self.data_root_edit = QLineEdit()
        self.data_root_edit.setPlaceholderText("Engine data root (blank = default)")

        btn_data_root = QPushButton("Browse…")
        btn_data_root.clicked.connect(self._browse_data_root)

        row = QHBoxLayout()
        row.addWidget(QLabel("Data root:"))
        row.addWidget(self.data_root_edit, 1)
        row.addWidget(btn_data_root)
        box_layout.addLayout(row)

        # Archives root (optional)
        self.archives_root_edit = QLineEdit()
        self.archives_root_edit.setPlaceholderText(
            "Archives root override (blank = engine default)"
        )

        btn_archives = QPushButton("Browse…")
        btn_archives.clicked.connect(self._browse_archives_root)

        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Archives root:"))
        row2.addWidget(self.archives_root_edit, 1)
        row2.addWidget(btn_archives)
        box_layout.addLayout(row2)

        # Default run mode
        self.run_mode_combo = QComboBox()
        self.run_mode_combo.addItem("Plan only", "plan")
        self.run_mode_combo.addItem("Materialize", "materialize")
        self.run_mode_combo.addItem("Execute", "execute")
        self.run_mode_combo.addItem("Execute + Compress", "execute+compress")

        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Default run mode:"))
        row3.addWidget(self.run_mode_combo, 1)
        box_layout.addLayout(row3)

        # Default compression
        self.compression_combo = QComboBox()
        self.compression_combo.addItem("None", "none")
        self.compression_combo.addItem("zip", "zip")
        self.compression_combo.addItem("tar.zst (preferred)", "tar.zst")

        row4 = QHBoxLayout()
        row4.addWidget(QLabel("Default compression:"))
        row4.addWidget(self.compression_combo, 1)
        box_layout.addLayout(row4)

        # Save button
        btn_save = QPushButton("Save Settings")
        btn_save.clicked.connect(self._save)
        box_layout.addWidget(btn_save)

        layout.addWidget(box)
        layout.addStretch(1)

        self._load_into_widgets()

    def _load_into_widgets(self) -> None:
        s = self._settings
        self.data_root_edit.setText("" if s.data_root is None else str(s.data_root))
        self.archives_root_edit.setText("" if s.archives_root is None else str(s.archives_root))

        self._select_combo_by_data(self.run_mode_combo, s.default_run_mode)
        self._select_combo_by_data(self.compression_combo, s.default_compression)

    @staticmethod
    def _select_combo_by_data(combo: QComboBox, value: str) -> None:
        for i in range(combo.count()):
            if str(combo.itemData(i)) == value:
                combo.setCurrentIndex(i)
                return

    def _browse_data_root(self) -> None:
        start_dir = self.data_root_edit.text().strip() or str(Path.home())
        directory = QFileDialog.getExistingDirectory(self, "Select data root folder", start_dir)
        if directory:
            self.data_root_edit.setText(directory)

    def _browse_archives_root(self) -> None:
        start_dir = self.archives_root_edit.text().strip() or str(Path.home())
        directory = QFileDialog.getExistingDirectory(self, "Select archives root folder", start_dir)
        if directory:
            self.archives_root_edit.setText(directory)

    def _save(self) -> None:
        data_root_text = self.data_root_edit.text().strip()
        archives_root_text = self.archives_root_edit.text().strip()

        settings = GuiSettings(
            data_root=Path(data_root_text) if data_root_text else None,
            archives_root=Path(archives_root_text) if archives_root_text else None,
            default_compression=str(self.compression_combo.currentData()),
            default_run_mode=str(self.run_mode_combo.currentData()),
        )

        try:
            save_gui_settings(data_root=None, settings=settings)
        except Exception as exc:
            QMessageBox.critical(self, "Settings", f"Failed to save settings: {exc}")
            return

        self._settings = settings
        QMessageBox.information(self, "Settings", "Saved.")
