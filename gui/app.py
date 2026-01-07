"""
WCBT GUI app.

Tabbed GUI backed by engine components (ProfileStore, backup/restore services).
"""

from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication, QHBoxLayout, QLabel, QTabWidget, QVBoxLayout, QWidget

from gui.tabs.authoring_tab import AuthoringTab
from gui.tabs.restore_tab import RestoreTab
from gui.tabs.run_tab import RunTab


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
    w = AppWindow()
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
