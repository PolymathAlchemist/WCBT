"""
WCBT GUI app.

Tabbed GUI backed by engine components (ProfileStore, backup/restore services).
"""

from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from backup_engine.profile_store.errors import IncompatibleProfileStoreError
from backup_engine.profile_store.sqlite_store import (
    profile_store_db_path,
    validate_profile_store_contract,
)
from gui.tabs.authoring_tab import AuthoringTab
from gui.tabs.restore_tab import RestoreTab
from gui.tabs.run_tab import RunTab
from gui.tabs.scheduling_tab import SchedulingTab
from gui.tabs.settings_tab import SettingsTab


def _show_legacy_database_recovery_modal(*, database_path: Path, details: str) -> bool:
    """
    Show a recovery modal for a pre-contract profile database.

    Parameters
    ----------
    database_path:
        Path to the incompatible SQLite database.
    details:
        Detailed incompatibility message for the user.

    Returns
    -------
    bool
        True when the user chooses Reset Database, otherwise False.
    """

    message_box = QMessageBox()
    message_box.setIcon(QMessageBox.Warning)
    message_box.setWindowTitle("Legacy Database Detected")
    message_box.setText(
        "This WCBT profile database was created before the current job/template contract "
        "and is no longer compatible."
    )
    message_box.setInformativeText(
        "Reset Database will delete the local SQLite database and remove old local profile "
        "data stored there.\n\n"
        f"Database path:\n{database_path}\n\n"
        "If a migration tool is added in a future build, you can use that instead."
    )
    message_box.setDetailedText(details)
    reset_button = message_box.addButton("Reset Database", QMessageBox.DestructiveRole)
    message_box.addButton(QMessageBox.Cancel)
    message_box.exec()
    return message_box.clickedButton() is reset_button


def _reset_profile_store_database(database_path: Path) -> None:
    """
    Delete the local profile store database and SQLite sidecar files.

    Parameters
    ----------
    database_path:
        Path to the profile store SQLite database.

    Notes
    -----
    SQLite WAL mode may leave ``-wal`` and ``-shm`` sidecar files beside the
    main database file. Removing all three keeps the reset deterministic.
    """

    for path in (
        database_path,
        database_path.with_name(f"{database_path.name}-wal"),
        database_path.with_name(f"{database_path.name}-shm"),
    ):
        if path.exists():
            path.unlink()


class AppWindow(QWidget):
    """
    Main window for the WCBT GUI.

    Responsibilities
    ----------------
    - Host the primary tabbed interface (Run, Restore, Authoring)
    - Coordinate clean shutdown of tab-owned background workers
    """

    def __init__(self) -> None:
        """
        Initialize the main window and construct the tab layout.
        """
        super().__init__()
        self.setWindowTitle("WCBT GUI")
        self.resize(1180, 720)

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)

        header = QWidget()
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(8, 8, 8, 8)

        title = QLabel("WCBT")
        f = title.font()
        f.setPointSize(16)
        f.setBold(True)
        title.setFont(f)

        subtitle = QLabel("Tabbed GUI backed by engine services")
        subtitle.setStyleSheet("color: #666;")

        header_layout.addWidget(title)
        header_layout.addSpacing(10)
        header_layout.addWidget(subtitle)
        header_layout.addStretch(1)

        root.addWidget(header)

        tabs = QTabWidget()

        self.run_tab = RunTab()
        tabs.addTab(self.run_tab, "Run")

        self.restore_tab = RestoreTab()
        tabs.addTab(self.restore_tab, "Restore")

        self.authoring_tab = AuthoringTab()
        tabs.addTab(self.authoring_tab, "Authoring")

        self.scheduling_tab = SchedulingTab()
        tabs.addTab(self.scheduling_tab, "Scheduling")

        self.settings_tab = SettingsTab()
        tabs.addTab(self.settings_tab, "Settings")

        root.addWidget(tabs, 1)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """
        Handle window close by shutting down background workers.

        Parameters
        ----------
        event:
            Qt close event.
        """
        try:
            if hasattr(self, "run_tab"):
                self.run_tab.shutdown()

            if hasattr(self, "restore_tab"):
                self.restore_tab.shutdown()

            if hasattr(self, "authoring_tab"):
                self.authoring_tab.shutdown()

            if hasattr(self, "scheduling_tab"):
                self.scheduling_tab.shutdown()

            if hasattr(self, "settings_tab"):
                # Settings tab has no worker threads today, but keep symmetry.
                pass

        finally:
            super().closeEvent(event)


def main() -> int:
    """
    Run the WCBT GUI application.

    Returns
    -------
    int
        Qt application exit code.
    """
    app = QApplication(sys.argv)
    profile_name = "default"
    data_root = None
    database_path = profile_store_db_path(profile_name=profile_name, data_root=data_root)

    while True:
        try:
            validate_profile_store_contract(profile_name=profile_name, data_root=data_root)
            break
        except IncompatibleProfileStoreError as exc:
            should_reset = _show_legacy_database_recovery_modal(
                database_path=database_path,
                details=str(exc),
            )
            if not should_reset:
                return 1
            try:
                _reset_profile_store_database(database_path)
            except OSError as reset_error:
                QMessageBox.critical(
                    None,
                    "Database Reset Failed",
                    "WCBT could not delete the incompatible local database.\n\n"
                    f"Database path:\n{database_path}\n\n"
                    "Close any other WCBT windows or tools using this database and try again.\n\n"
                    f"System error: {reset_error}",
                )
                return 1

    w = AppWindow()
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
